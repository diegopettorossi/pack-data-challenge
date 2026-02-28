"""
ingest.py — Load raw data sources into DuckDB (raw layer).

Idempotency: users/events use incremental INSERT WHERE NOT EXISTS; mentors
are always full-replaced (small ref table, tier changes must propagate).
"""

import json
import logging
from pathlib import Path

import duckdb
import polars as pl

from validate import (
    validate_csv,
    validate_json_file,
    detect_csv_duplicates,
)

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH  = BASE_DIR / "warehouse.duckdb"


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    """Check if table exists in database."""
    row_count_query = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [name]
    ).fetchone()
    row_count = row_count_query[0] if row_count_query else 0
    return row_count > 0


def _incremental_insert(
    con: duckdb.DuckDBPyConnection, df: pl.DataFrame, pk: str, table: str
) -> int:
    """Insert rows from df whose pk doesn't exist in table. Returns new row count."""
    con.register("_staged", df)
    new_rows = con.execute(f"""
        SELECT COUNT(*) FROM _staged s
        WHERE NOT EXISTS (SELECT 1 FROM {table} r WHERE r.{pk} = s.{pk})
    """).fetchone()
    new_rows = new_rows[0] if new_rows else 0
    if new_rows > 0:
        con.execute(f"""
            INSERT INTO {table}
            SELECT s.* FROM _staged s
            WHERE NOT EXISTS (SELECT 1 FROM {table} r WHERE r.{pk} = s.{pk})
        """)
    con.unregister("_staged")
    return new_rows


def ingest_users(con: duckdb.DuckDBPyConnection) -> int:
    """Load users CSV → raw_users. Returns new row count."""

    src = DATA_DIR / "users_db_export.csv"
    issues = validate_csv(src, required_columns={"user_id"})
    if issues:
        for issue in issues:
            log.error(issue)
        raise ValueError(
            f"users_db_export.csv failed pre-flight validation: {issues[0]}"
        )
    total_rows, dup_count = detect_csv_duplicates(src, "user_id")
    if dup_count > 0:
        log.warning(
            f"[DuplicateCheck] users_db_export.csv: {dup_count}/{total_rows} duplicate "
            f"user_ids detected in source file — will be deduplicated during ingestion."
        )

    # Read as Utf8 first to handle mixed int/str encoding, then cast.
    df = pl.read_csv(src, schema_overrides={"user_id": pl.Utf8})
    log.info(f"Users raw rows: {len(df)}")

    df = df.with_columns(pl.col("user_id").cast(pl.Int64, strict=False))
    bad = df["user_id"].is_null().sum()
    if bad:
        log.warning(f"Dropped {bad} rows with unparseable user_id")
        df = df.drop_nulls(subset=["user_id"])

    before = len(df)
    df = df.unique(subset=["user_id"], keep="first", maintain_order=True)
    log.info(f"Users: removed {before - len(df)} source duplicates → {len(df)} unique")

    df = df.with_columns([
        pl.col("company_id").cast(pl.Int64),
        pl.col("signup_date").str.to_datetime(strict=False),
        pl.col("status").str.strip_chars().str.to_lowercase(),
    ])

    if not _table_exists(con, "raw_users"):
        con.register("_users", df)
        con.execute("CREATE TABLE raw_users AS SELECT * FROM _users")
        con.unregister("_users")
        log.info(f"✓ raw_users created ({len(df)} rows)")
        return len(df)

    new_rows = _incremental_insert(con, df, "user_id", "raw_users")
    log.info(f"✓ raw_users incremental: {new_rows} new rows")
    return new_rows


def ingest_mentors(con: duckdb.DuckDBPyConnection) -> None:
    """Load mentors CSV → raw_mentors. Always full-replace."""
    src = DATA_DIR / "mentor_tiers.csv"
    issues = validate_csv(src, required_columns={"mentor_id", "tier", "hourly_rate"})
    if issues:
        for issue in issues:
            log.error(issue)
        raise ValueError(
            f"mentor_tiers.csv failed pre-flight validation: {issues[0]}"
        )

    df = pl.read_csv(src)
    log.info(f"Mentors raw rows: {len(df)}")

    df = df.with_columns([
        pl.col("mentor_id").cast(pl.Utf8).str.strip_chars(),
        pl.col("tier").cast(pl.Utf8).str.strip_chars().str.to_titlecase(),
        pl.col("hourly_rate").cast(pl.Int64, strict=False).fill_null(0),
    ])

    null_tiers = df["tier"].is_null().sum()
    if null_tiers > 0:
        raise ValueError(
            f"mentor_tiers.csv: {null_tiers} rows with null tier after normalisation."
        )

    con.execute("DROP TABLE IF EXISTS raw_mentors")
    con.register("_mentors", df)
    con.execute("CREATE TABLE raw_mentors AS SELECT * FROM _mentors")
    con.unregister("_mentors")
    log.info(f"✓ raw_mentors replaced ({len(df)} rows)")


def ingest_events(con: duckdb.DuckDBPyConnection) -> int:
    """Load booking events JSON → raw_events. Returns new row count."""

    src = DATA_DIR / "booking_events.json"
    issues = validate_json_file(src)
    if issues:
        for issue in issues:
            log.error(issue)
        raise ValueError(
            f"booking_events.json failed pre-flight validation: {issues[0]}"
        )

    with open(src, encoding="utf-8") as f:
        events = json.load(f)

    # Normalise user_id: source mixes int and str.
    for e in events:
        if "user_id" in e:
            e["user_id"] = str(e["user_id"])

    df = pl.DataFrame(events)
    log.info(f"Events raw rows: {len(df)}")

    df = df.with_columns(pl.col("user_id").cast(pl.Int64, strict=False))
    bad = df["user_id"].is_null().sum()
    if bad:
        log.warning(f"Dropped {bad} events with unparseable user_id")
        df = df.drop_nulls(subset=["user_id"])

    dup = df["event_id"].is_duplicated().sum()
    if dup:
        log.warning(f"Found {dup} duplicate event_ids — deduplicating")
        df = df.unique(subset=["event_id"], keep="first", maintain_order=True)

    df = df.with_columns([
        pl.col("mentor_id").str.strip_chars(),
        pl.col("timestamp").str.to_datetime(strict=False, time_zone="UTC"),
        pl.col("event_type").str.strip_chars().str.to_lowercase(),
    ])

    # ── DQ: unknown event types ──────────────────────────────────────────
    _KNOWN_EVENT_TYPES = {
        "booking_requested", "booking_confirmed", "booking_cancelled",
        "session_started",   "session_ended",
    }
    unknown_events = df.filter(~pl.col("event_type").is_in(list(_KNOWN_EVENT_TYPES)))
    if len(unknown_events) > 0:
        unknown_counts = (
            unknown_events.group_by("event_type")
            .len()
            .sort("len", descending=True)
        )
        for row in unknown_counts.iter_rows(named=True):
            log.warning(
                f"[DQ] Unknown event_type '{row['event_type']}': "
                f"{row['len']} events — these rows will be stored but "
                f"excluded from staging models."
            )

    # ── DQ: booking events without a known user ──────────────────────────
    booking_no_user = df.filter(
        pl.col("event_type").str.starts_with("booking_") & pl.col("user_id").is_null()
    )
    if len(booking_no_user) > 0:
        log.warning(
            f"[DQ] {len(booking_no_user)} booking events have null user_id "
            f"after parsing — will be dropped."
        )

    if not _table_exists(con, "raw_events"):
        con.register("_events", df)
        con.execute("CREATE TABLE raw_events AS SELECT * FROM _events")
        con.unregister("_events")
        log.info(f"✓ raw_events created ({len(df)} rows)")
        return len(df)

    new_rows = _incremental_insert(con, df, "event_id", "raw_events")
    log.info(f"✓ raw_events incremental: {new_rows} new rows")
    return new_rows


def main() -> dict:
    log.info("=== Ingestion starting ===")
    con = duckdb.connect(str(DB_PATH))
    try:
        new_users  = ingest_users(con)
        ingest_mentors(con)
        new_events = ingest_events(con)
        for t in ["raw_users", "raw_mentors", "raw_events"]:

            row_count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
            row_count = row_count[0] if row_count else 0
            log.info(f"  {t}: {row_count} total rows")
        log.info("=== Ingestion complete ===")
        return {"new_users": new_users, "new_events": new_events}
    finally:
        con.close()


if __name__ == "__main__":
    main()
