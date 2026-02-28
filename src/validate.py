"""
validate.py — Pre-flight file validators and post-transform DQ checks.

run_checks() is called by run_pipeline.py after dbt has already run its
schema tests (referential integrity, not-null, PK uniqueness). The checks
here cover business-logic rules that dbt can't express declaratively.
"""

import csv
import json
import sys
import logging
from pathlib import Path
from typing import Optional

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "warehouse.duckdb"

MAX_DURATION_MINS = 240  # fallback when running standalone without a config


class DataQualityError(Exception):
    """Raised for infrastructure failures during validation (DB errors, None results)."""


def check_encoding(path: Path) -> list[str]:
    """Return a non-empty error list if *path* is not valid UTF-8."""
    try:
        path.read_text(encoding="utf-8", errors="strict")
        return []
    except UnicodeDecodeError as exc:
        return [f"[EncodingCheck] '{path.name}' non-UTF-8 at byte {exc.start}: {exc.reason}"]
    except OSError as exc:
        return [f"[EncodingCheck] Cannot read '{path.name}': {exc}"]


def validate_csv(path: Path, required_columns: set[str]) -> list[str]:
    """Check that a CSV exists, is UTF-8, has the required columns, and has at least one data row."""
    if not path.exists():
        return [f"[FileFormat] '{path.name}' not found at {path}"]
    issues = check_encoding(path)
    if issues:
        return issues
    try:
        with open(path, encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            missing = required_columns - set(reader.fieldnames or [])
            if missing:
                issues.append(
                    f"[FileFormat] '{path.name}' missing columns: {sorted(missing)}"
                )
            try:
                next(reader)
            except StopIteration:
                issues.append(f"[FileFormat] '{path.name}' has no data rows.")
    except Exception as exc:  # noqa: BLE001
        issues.append(f"[FileFormat] '{path.name}' read error: {exc}")
    return issues


def validate_json_file(path: Path) -> list[str]:
    """Check that a JSON file exists, is UTF-8, and is a non-empty array."""
    if not path.exists():
        return [f"[FileFormat] '{path.name}' not found at {path}"]
    issues = check_encoding(path)
    if issues:
        return issues
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            issues.append(f"[FileFormat] '{path.name}' must be a JSON array, got {type(data).__name__}.")
        elif not data:
            issues.append(f"[FileFormat] '{path.name}' is an empty JSON array.")
    except json.JSONDecodeError as exc:
        issues.append(f"[FileFormat] '{path.name}' JSON error at line {exc.lineno}: {exc.msg}")
    return issues


def detect_csv_duplicates(path: Path, pk_col: str) -> tuple[int, int]:
    """Return (total_rows, duplicate_pk_count) for the given primary-key column."""
    seen: set[str] = set()
    total = dupes = 0
    try:
        with open(path, encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                total += 1
                val: str = row.get(pk_col) or ""
                if val in seen:
                    dupes += 1
                else:
                    seen.add(val)
    except Exception as exc:  # noqa: BLE001
        log.warning(f"[DuplicateCheck] Could not scan '{path.name}': {exc}")
    return total, dupes


def warn_if_overwrite(path: Path, label: str = "") -> list[str]:
    """Return a warning message if *path* already exists."""
    if path.exists():
        tag = f" [{label}]" if label else ""
        return [f"[OutputPath]{tag} '{path.name}' already exists and will be overwritten."]
    return []


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    """Return True if *name* exists as a table in the connected DuckDB schema."""
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [name]
    ).fetchone()
    return bool(row and row[0])


def safe_fetchone(result, query_name: str = "query") -> Optional[tuple]:
    try:
        if result is None:
            raise DataQualityError(f"[{query_name}] DuckDB returned None")
        return result.fetchone()
    except duckdb.Error as e:
        raise DataQualityError(f"[{query_name}] DuckDB error: {e}") from e


def safe_fetchall(result, query_name: str = "query") -> list:
    try:
        if result is None:
            raise DataQualityError(f"[{query_name}] DuckDB returned None")
        rows = result.fetchall()
        return rows if rows is not None else []
    except duckdb.Error as e:
        raise DataQualityError(f"[{query_name}] DuckDB error: {e}") from e


def run_checks(con: duckdb.DuckDBPyConnection, config=None) -> tuple[list[str], list[str]]:
    """Run all DQ checks. Returns (failures, warnings)."""
    max_duration_mins = config.dq_max_duration_minutes if config else MAX_DURATION_MINS
    failures = []
    warnings = []

    try:
        # 1. no negative durations
        result = con.execute("""
            SELECT COUNT(*) FROM fct_sessions
            WHERE duration_minutes < 0
        """)
        row = safe_fetchone(result, "Check 1: negative durations")
        if row is None:
            raise DataQualityError("Check 1: empty result from COUNT query")
        neg_count = row[0]
        if neg_count is None:
            raise DataQualityError("Check 1: COUNT(*) returned None")
        if neg_count > 0:
            failures.append(
                f"FAIL [Check 1]: {neg_count} sessions have negative duration. "
                f"This indicates session_ended < session_started (data corruption)."
            )
        else:
            log.info("PASS [Check 1]: No negative session durations")

        # 4. duration outliers — warn only, don't halt
        result = con.execute(f"""
            SELECT COUNT(*) FROM fct_sessions
            WHERE duration_minutes > {max_duration_mins}
        """)
        check4_result = safe_fetchone(result, "Check 4: duration outliers")
        if check4_result is None:
            raise DataQualityError("Check 4: empty result from outlier check")
        outlier_count = check4_result[0]
        if outlier_count is None:
            raise DataQualityError("Check 4: outlier count is None")

        if outlier_count > 0:
            warnings.append(
                f"WARN [Check 4]: {outlier_count} sessions exceed {max_duration_mins} min — likely clock drift."
            )
        else:
            log.info(f"PASS [Check 4]: No sessions exceed {max_duration_mins} min")

        # 5. booking orphan rate — booking_requested with no confirmed/cancelled outcome.
        # Flag: run only when fct_bookings has been materialised by dbt.
        has_fct_bookings = _table_exists(con, "fct_bookings")
        if has_fct_bookings:
            result = con.execute("""
                SELECT
                    COUNT(*)                                            AS total_bookings,
                    SUM(CASE WHEN is_orphan_request THEN 1 ELSE 0 END) AS orphan_count
                FROM fct_bookings
            """)
            row = safe_fetchone(result, "Check 5: booking orphan rate")
            if row is None:
                raise DataQualityError("Check 5: empty result from orphan check")
            total_bookings, orphan_count = row[0], row[1]
            if total_bookings is None or orphan_count is None:
                raise DataQualityError("Check 5: NULL values returned from orphan check")
            if total_bookings > 0:
                orphan_rate = orphan_count / total_bookings
                threshold   = config.dq_max_orphan_rate if config else 0.05
                if orphan_rate > threshold:
                    warnings.append(
                        f"WARN [Check 5]: {orphan_count}/{total_bookings} bookings "
                        f"({orphan_rate:.1%}) are orphan requests (no confirmed/cancelled "
                        f"follow-up). Threshold is {threshold:.0%}. "
                        f"Possible cause: out-of-order events or missing outcome records."
                    )
                else:
                    log.info(
                        f"PASS [Check 5]: Booking orphan rate {orphan_rate:.1%} "
                        f"within threshold ({threshold:.0%}). "
                        f"{orphan_count}/{total_bookings} orphan requests."
                    )
            else:
                log.info("SKIP [Check 5]: fct_bookings is empty — no orphan check needed.")
        else:
            log.info("SKIP [Check 5]: fct_bookings not yet materialised — run dbt first.")

        # 6. configured tier names must exist in dim_mentors — catches config.yaml typos
        if config is not None:
            all_configured = list(config.tiers_group_a) + list(config.tiers_group_b)
            
            if not all_configured:
                log.warning("WARN [Check 6]: No tiers configured in config.yaml")
            else:
                escaped = [t.replace("'", "''") for t in all_configured]
                placeholders = ", ".join(f"'{t}'" for t in escaped)
                
                existing = {r[0] for r in safe_fetchall(
                    con.execute(f"SELECT DISTINCT tier FROM dim_mentors WHERE tier IN ({placeholders})"),
                    "Check 6: existing tiers",
                )}
                available = [r[0] for r in safe_fetchall(
                    con.execute("SELECT DISTINCT tier FROM dim_mentors ORDER BY tier"),
                    "Check 6: available tiers",
                )]
                missing_tiers = [t for t in all_configured if t not in existing]
                if missing_tiers:
                    failures.append(
                        f"FAIL [Check 6]: Configured tier(s) {missing_tiers} not found in "
                        f"dim_mentors (available: {available}). Check config.yaml for typos."
                    )
                else:
                    log.info("PASS [Check 6]: All configured tiers exist in dim_mentors")
        else:
            log.info("SKIP [Check 6]: No config — skipping tier existence check")

        for w in warnings:
            log.warning(w)
        return failures, warnings

    except DataQualityError:
        raise
    except duckdb.Error as e:
        raise DataQualityError(f"Database error: {e}") from e
    except Exception as e:
        raise DataQualityError(f"Unexpected error: {e}") from e


def main():
    log.info("=== Data Quality Validation ===")
    con = None
    try:
        con = duckdb.connect(str(DB_PATH), read_only=True)
        failures, dq_warnings = run_checks(con)

        if failures:
            log.error("=" * 60)
            log.error("PIPELINE HALTED — Data quality checks failed:")
            for f in failures:
                log.error(f"  • {f}")
            log.error("=" * 60)
            sys.exit(1)
        else:
            log.info("=== All data quality checks passed ===")

    except DataQualityError as e:
        log.error(f"Data quality validation failed: {e}")
        sys.exit(2)
    except Exception as e:
        log.error(f"Unexpected error: {e}")
        sys.exit(2)
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    main()