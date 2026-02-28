"""
run_pipeline.py — End-to-end orchestrator for the Pack data challenge.

Auto-installs dependencies on first run (no manual pip install needed).

Steps:
  1. Ingest raw data into DuckDB         (src/ingest.py)
  2. dbt snapshot + dbt run              (dbt/ project, vars injected from PipelineConfig)
  3. dbt test + Python DQ checks         (src/validate.py)
  4. Compile + execute dbt analyses      (dbt/analyses/, executed against DuckDB directly)
  5. Record run in JSON log              (logs/YYYY/MM/YYYYMMDD.json)

  mode='full'            → all steps
  mode='ingest-only'     → step 1 only
  mode='transform-only'  → steps 2 + 3
  mode='analysis-only'   → step 4 only (requires models already built)

Returns a results dict so app.py can consume it without re-running the pipeline.
Raises RuntimeError on failure — never calls sys.exit so Streamlit stays alive.
"""

# Auto-install dependencies before any third-party import.  subprocess/sys are stdlib.
import subprocess
import sys

# (module_name, pip_package_name) — PyYAML imports as 'yaml', not 'pyyaml'.
_REQUIRED = [("duckdb", "duckdb"), ("polars", "polars"), ("yaml", "pyyaml")]
for _mod, _pkg in _REQUIRED:
    try:
        __import__(_mod)
    except ImportError:
        print(f"[bootstrap] Installing missing package: {_pkg}")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", _pkg],
            stdout=subprocess.DEVNULL,
        )

# ── dbt invocation ────────────────────────────────────────────────────────────
# Always call dbt via `python -c "from dbt.cli.main import cli; cli()"`.
# This is more reliable than the .exe launcher wrapper which can reference a
# stale Python path when the venv has been moved or recreated.
_DBT_PREFIX: list[str] = [sys.executable, "-c", "from dbt.cli.main import cli; cli()"]

# Ensure dbt-duckdb is installed.
try:
    subprocess.check_call(
        [*_DBT_PREFIX, "--version"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
except (subprocess.CalledProcessError, FileNotFoundError):
    print("[bootstrap] Installing missing package: dbt-duckdb")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--quiet", "dbt-duckdb"],
        stdout=subprocess.DEVNULL,
    )

import argparse
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

BASE_DIR   = Path(__file__).resolve().parent
DB_PATH    = BASE_DIR / "warehouse.duckdb"
LOGS_DIR   = BASE_DIR / "logs"   # logs/YYYY/MM/YYYYMMDD.json

# Add src/ and the solution root so ingest/validate/utils/config are importable.
sys.path.insert(0, str(BASE_DIR / "src"))
sys.path.insert(0, str(BASE_DIR))

from config import PipelineConfig
from utils import PipelineTimeoutError, StepTimer
from validate import warn_if_overwrite


def _wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score 95% confidence interval for a proportion k/n, returned as percentages."""
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    denom  = 1 + z ** 2 / n
    centre = (p + z ** 2 / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z ** 2 / (4 * n ** 2)) / denom
    return (max(0.0, centre - margin) * 100, min(100.0, centre + margin) * 100)



class PipelineRunLog:
    """Appends one JSON entry per run to logs/YYYY/MM/YYYYMMDD.json (atomic write)."""

    _FMT = "%Y-%m-%dT%H:%M:%SZ"  # UTC, trimmed to seconds

    def __init__(self, config: PipelineConfig) -> None:
        self._started_at = datetime.now(tz=timezone.utc)
        self._warnings: list[str] = []
        self._log_path = (
            LOGS_DIR
            / self._started_at.strftime("%Y")
            / self._started_at.strftime("%m")
            / f"{self._started_at.strftime('%Y%m%d')}.json"
        )
        self._run_id = self._next_run_id()
        self._entry: dict = {
            "run_id":              self._run_id,
            "started_at":          self._started_at.strftime(self._FMT),
            "finished_at":         None,
            "duration_seconds":    None,
            "status":              "running",
            "config":              json.loads(config.to_json()),
            "new_users_ingested":  0,
            "new_events_ingested": 0,
            "dq_checks_passed":    False,
            "warnings":            [],
            "error_message":       None,
        }

    def _next_run_id(self) -> int:
        """Count all entries across existing daily log files and increment."""
        total = 0
        for p in LOGS_DIR.rglob("*.json"):
            try:
                total += len(json.loads(p.read_text(encoding="utf-8")))
            except Exception:  # noqa: BLE001
                pass
        return total + 1

    def _read_day(self) -> list[dict]:
        if self._log_path.exists():
            try:
                return json.loads(self._log_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                pass
        return []

    def _write_atomic(self, entries: list[dict]) -> None:
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._log_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(entries, indent=2, default=str), encoding="utf-8")
        tmp.replace(self._log_path)

    def start(self) -> None:
        entries = self._read_day()
        entries.append(self._entry)
        self._write_atomic(entries)
        log.info(f"Run log → {self._log_path} (run_id={self._run_id})")

    def add_warning(self, msg: str) -> None:
        self._warnings.append(msg)

    def finish(
        self,
        *,
        status: str,
        new_users: int = 0,
        new_events: int = 0,
        dq_passed: bool = False,
        error: str | None = None,
    ) -> None:
        finished_at = datetime.now(tz=timezone.utc)
        self._entry.update({
            "finished_at":         finished_at.strftime(self._FMT),
            "duration_seconds":    round((finished_at - self._started_at).total_seconds(), 1),
            "status":              status,
            "new_users_ingested":  new_users,
            "new_events_ingested": new_events,
            "dq_checks_passed":    dq_passed,
            "warnings":            self._warnings,
            "error_message":       error,
        })
        entries = self._read_day()
        for i, e in enumerate(entries):
            if e.get("run_id") == self._run_id:
                entries[i] = self._entry
                break
        else:
            entries.append(self._entry)
        self._write_atomic(entries)
        log.info(f"Run log finalised (status={status}, run_id={self._run_id})")

    @property
    def run_id(self) -> int:
        return self._run_id

    @property
    def log_path(self) -> Path:
        return self._log_path



def main(config: PipelineConfig | None = None, mode: str = "full") -> dict | None:
    """
    Run the pipeline.

    mode options:
      'full'            — ingest → transform → validate → analyse
      'ingest-only'     — raw data load only
      'transform-only'  — dbt snapshot + run + test (assumes raw data already loaded)
      'analysis-only'   — compile + execute analyses only (assumes models already built)
    """
    if config is None:
        config = PipelineConfig.from_yaml()
    config.validate()

    log.info("=" * 60)
    log.info("PACK DATA CHALLENGE — Pipeline Start")
    log.info(f"mode={mode}  config={config.to_json()}")
    log.info("=" * 60)

    run_log = PipelineRunLog(config)
    run_log.start()
    log.info(f"Pipeline run_id: {run_log.run_id}")

    for w in warn_if_overwrite(DB_PATH, label="DuckDB warehouse"):
        log.info(w)

    new_users  = 0
    new_events = 0
    dq_passed  = False
    timeout    = config.step_timeout_seconds if config.step_timeout_seconds > 0 else None

    # Single var dict injected into every dbt invocation — one source of truth.
    # Extra keys are silently ignored by subcommands that don't declare them.
    _dbt_vars = json.dumps({
        "default_duration_minutes": config.default_session_duration_minutes,
        "tiers_group_a":            config.tiers_group_a,
        "tiers_group_b":            config.tiers_group_b,
        "rebooking_window_days":    config.rebooking_window_days,
    })
    dbt_dir   = BASE_DIR / "dbt"
    _analyses = dbt_dir / "analyses"   # plain-SQL templates — no dbt compile needed

    con = duckdb.connect(str(DB_PATH))
    try:
        step_label = {
            "full":            "Full pipeline",
            "ingest-only":     "Ingest only",
            "transform-only":  "Transform only",
            "analysis-only":   "Analysis only",
        }[mode]

        with StepTimer(step_label, timeout_seconds=timeout):

            # ── STEP 1: Ingest ───────────────────────────────────────────────
            if mode in ("full", "ingest-only"):
                log.info("\n[Step 1/4] Ingesting raw data...")
                from ingest import main as ingest_main
                ingest_result = ingest_main()
                new_users  = ingest_result.get("new_users",  0)
                new_events = ingest_result.get("new_events", 0)
                log.info(f"  Ingested: {new_users} users, {new_events} events.")

                if mode == "ingest-only":
                    run_log.finish(
                        status="success",
                        new_users=new_users, new_events=new_events, dq_passed=False,
                    )
                    return {"new_users": new_users, "new_events": new_events, "mode": mode}

            # ── STEP 2: dbt snapshot + run ───────────────────────────────────
            if mode in ("full", "transform-only"):
                log.info("\n[Step 2/4] Building dimension and fact tables (dbt)...")
                # DuckDB only allows one writer — close before spawning subprocess.
                con.close()
                con = None

                for dbt_cmd in [
                    [*_DBT_PREFIX, "snapshot", "--profiles-dir", ".", "--vars", _dbt_vars],
                    [*_DBT_PREFIX, "run",      "--profiles-dir", ".", "--vars", _dbt_vars],
                ]:
                    result = subprocess.run(
                        dbt_cmd, cwd=str(dbt_dir), capture_output=True, text=True
                    )
                    log.info(result.stdout)
                    if result.returncode != 0:
                        log.error(result.stderr)
                        raise RuntimeError(
                            f"dbt failed: {' '.join(dbt_cmd)}\n{result.stderr}"
                        )

                # Reacquire connection and log row counts.
                con = duckdb.connect(str(DB_PATH))
                log.info("\n  Model row counts:")
                for table in ["dim_users", "dim_mentors", "fct_sessions", "fct_bookings"]:
                    n = (con.execute(f"SELECT COUNT(*) FROM {table}").fetchone() or (0,))[0]
                    log.info(f"    {table}: {n} rows")

            # ── STEP 3: dbt test + Python DQ checks ─────────────────────────
            if mode in ("full", "transform-only"):
                log.info("\n[Step 3/4] Running data quality checks...")
                con.close()
                con = None

                dbt_test = subprocess.run(
                    [*_DBT_PREFIX, "test", "--profiles-dir", ".", "--vars", _dbt_vars],
                    cwd=str(dbt_dir), capture_output=True, text=True,
                )
                log.info(dbt_test.stdout)
                if dbt_test.returncode != 0:
                    log.error(dbt_test.stderr)
                    failures_dbt = [
                        ln.strip()
                        for ln in dbt_test.stdout.splitlines()
                        if " FAIL " in ln
                    ]
                    raise RuntimeError(
                        "; ".join(failures_dbt) or dbt_test.stderr
                    )

                con = duckdb.connect(str(DB_PATH))
                from validate import run_checks
                failures, dq_warnings = run_checks(con, config)
                for w in dq_warnings:
                    run_log.add_warning(w)

                if failures:
                    log.error("\n" + "=" * 60)
                    log.error("PIPELINE HALTED — Data quality checks failed:")
                    for f in failures:
                        log.error(f"  • {f}")
                    log.error("=" * 60)
                    raise RuntimeError("; ".join(failures))

                dq_passed = True

                if mode == "transform-only":
                    log.info("\n[transform-only] Skipping analysis.")
                    run_log.finish(
                        status="success",
                        new_users=new_users, new_events=new_events, dq_passed=True,
                    )
                    return {
                        "new_users": new_users, "new_events": new_events,
                        "dq_passed": True, "mode": mode, "config": config,
                    }

            # analysis-only skips DQ but marks it passed so the rest continues.
            if mode == "analysis-only":
                dq_passed = True

            # ── STEP 4: execute analysis queries ─────────────────────────────
            # The analyses are plain-SQL templates in dbt/analyses/.  We substitute
            # {PLACEHOLDER} tokens at runtime — no dbt compile step needed.
            log.info("\n[Step 4/4] Running analyses...")
            if con is not None:
                con.close()
                con = None

            con = duckdb.connect(str(DB_PATH))

            _rebooking_sql = (_analyses / "analysis_rebooking.sql").read_text(encoding="utf-8")
            for _ph, _val in {
                "TIERS_A":          config.tiers_a_sql(),
                "TIERS_B":          config.tiers_b_sql(),
                "GROUP_A_LABEL":    config.group_a_label(),
                "GROUP_B_LABEL":    config.group_b_label(),
                "WINDOW_CONDITION": config.window_condition_sql(),
            }.items():
                _rebooking_sql = _rebooking_sql.replace(f"{{{_ph}}}", _val)

            rebooking_df = con.sql(_rebooking_sql).pl()

            # Sanity check: extreme rates on a large group indicate a data issue.
            _MIN_USERS = 30
            for _row in rebooking_df.iter_rows(named=True):
                rate = _row["rebooking_rate_pct"]
                n    = _row["total_users"]
                tier = _row["mentor_tier"]
                if n < _MIN_USERS:
                    msg = (
                        f"WARN [Analysis]: Group '{tier}' has only {n} users — "
                        f"rebooking rate is directional only (need ≥{_MIN_USERS})."
                    )
                    log.warning(msg); run_log.add_warning(msg)
                elif rate == 100.0:
                    msg = f"WARN [Analysis]: '{tier}' shows 100% rebooking rate — verify sample size and date range."
                    log.warning(msg); run_log.add_warning(msg)
                elif rate == 0.0:
                    msg = f"WARN [Analysis]: '{tier}' shows 0% rebooking rate across {n} users — check session data."
                    log.warning(msg); run_log.add_warning(msg)

            ci_rows: list[dict] = []
            for _row in rebooking_df.iter_rows(named=True):
                lo, hi = _wilson_ci(int(_row["users_rebooked"]), int(_row["total_users"]))
                ci_rows.append({**_row, "ci_lower_pct": round(lo, 1), "ci_upper_pct": round(hi, 1)})

            log.info("\n" + "=" * 60)
            log.info("CEO ANSWER: Rebooking Rate by Mentor Tier")
            log.info("=" * 60)
            _hdr = f"{'Tier':<20} {'Users':>6} {'Rebooked':>9} {'Rate':>7}  95% CI (Wilson)"
            log.info(_hdr)
            log.info("-" * len(_hdr))
            for _r in ci_rows:
                log.info(
                    f"{str(_r['mentor_tier']):<20} {_r['total_users']:>6} "
                    f"{_r['users_rebooked']:>9} {_r['rebooking_rate_pct']:>6.1f}%"
                    f"  [{_r['ci_lower_pct']:.1f}% \u2013 {_r['ci_upper_pct']:.1f}%]"
                )
            log.info("=" * 60)

            booking_reliability_df = con.sql(
                (_analyses / "booking_reliability.sql").read_text(encoding="utf-8")
            ).pl()
            log.info(f"\nBooking reliability by tier:\n{booking_reliability_df}")

            log.info("\n" + "=" * 60)
            log.info("PIPELINE COMPLETE")
            log.info("=" * 60)

            run_log.finish(
                status="success",
                new_users=new_users, new_events=new_events, dq_passed=True,
            )

            return {
                "rebooking_df":           rebooking_df,
                "ci_rows":                ci_rows,
                "booking_reliability_df": booking_reliability_df,
                "new_users":              new_users,
                "new_events":             new_events,
                "run_id":                 run_log.run_id,
                "log_path":               str(run_log.log_path),
                "config":                 config,
            }

    except PipelineTimeoutError as exc:
        log.error(f"TIMEOUT: {exc}")
        try:
            run_log.finish(
                status="timeout",
                new_users=new_users, new_events=new_events,
                dq_passed=dq_passed, error=str(exc),
            )
        except Exception:
            pass
        raise

    except Exception as exc:
        try:
            run_log.finish(
                status="failed",
                new_users=new_users, new_events=new_events,
                dq_passed=dq_passed, error=str(exc),
            )
        except Exception:
            pass
        raise

    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pack Data Challenge Pipeline — Ingest, Transform, Validate, Analyze",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python run_pipeline.py                  # Full pipeline (default)
  python run_pipeline.py --mode ingest-only      # Load raw data only
  python run_pipeline.py --mode transform-only   # dbt run + dbt test (skip ingest)
  python run_pipeline.py --mode analysis-only    # CEO query only (skip ingest + transform)
        """
    )
    parser.add_argument(
        "--mode",
        choices=["full", "ingest-only", "transform-only", "analysis-only"],
        default="full",
        help="Pipeline mode (default: full)"
    )
    args = parser.parse_args()
    main(mode=args.mode)

