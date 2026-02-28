"""utils.py — Shared pipeline utilities: timeout enforcement and warehouse cleanup."""

import ctypes
import logging
import os
import sys
import threading
from pathlib import Path

log = logging.getLogger(__name__)

# All tables managed by the pipeline (raw staging + transformed).
_TABLES = ["fct_sessions", "dim_users", "dim_mentors", "raw_events", "raw_users", "raw_mentors"]


class PipelineTimeoutError(RuntimeError):
    """Raised when a pipeline step exceeds its wall-clock budget."""


class StepTimer:
    """
    Context manager that enforces a per-step wall-clock timeout.

    Two-tier kill mechanism:
      1. Soft kill: PyThreadState_SetAsyncExc injects PipelineTimeoutError into the
         calling thread at the next Python bytecode boundary.
      2. Hard kill: if the soft kill hasn't taken effect within GRACE_SECONDS,
         os._exit(1) terminates the process unconditionally. This handles the case
         where the thread is blocked inside a C-level call (e.g. a long DuckDB query)
         that never yields back to the Python interpreter.

    CPython-specific. Windows-safe (no signal.alarm). Pass None or 0 to disable.
    """

    GRACE_SECONDS = 10

    def __init__(self, step_name: str, timeout_seconds: float | None):
        self._name    = step_name
        self._timeout = timeout_seconds
        self._done    = threading.Event()
        self._main_thread_id: int | None = None
        self._watcher: threading.Thread | None = None

    def _watchdog(self) -> None:
        if not self._timeout:
            return
        if self._done.wait(self._timeout):
            return  # step finished in time

        # Soft kill: inject exception into the Python thread.
        if self._main_thread_id is not None:
            log.error(
                f"[Timeout] '{self._name}' exceeded {self._timeout}s "
                "— sending PipelineTimeoutError (soft kill)."
            )
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(self._main_thread_id),
                ctypes.py_object(PipelineTimeoutError),
            )

        # Hard kill: if the soft kill didn't take effect within the grace period,
        # force exit. This is the safety net for C-level blocking calls.
        if not self._done.wait(self.GRACE_SECONDS):
            log.error(
                f"[Timeout] '{self._name}' soft kill unresponsive after "
                f"{self.GRACE_SECONDS}s grace — forcing os._exit(1)."
            )
            os._exit(1)

    def __enter__(self) -> "StepTimer":
        if self._timeout:
            self._main_thread_id = threading.current_thread().ident
            self._watcher = threading.Thread(
                target=self._watchdog, daemon=True, name=f"timeout-{self._name}"
            )
            self._watcher.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._done.set()
        if self._watcher is not None:
            self._watcher.join(timeout=2)
        if exc_type is PipelineTimeoutError:
            raise PipelineTimeoutError(
                f"Step '{self._name}' exceeded the {self._timeout}s timeout. "
                "Increase guardrails.step_timeout_seconds in config.yaml if needed."
            ) from exc_val
        return False


def clean_history(db_path: Path) -> None:
    """
    Drop all pipeline tables (raw + dim + fact) from the DuckDB warehouse.

    Use this to reset the warehouse to a blank state before a fresh pipeline run,
    or to clear test data. The warehouse file itself is not deleted.
    """
    import duckdb
    log.info(f"Cleaning warehouse: {db_path}")
    con = duckdb.connect(str(db_path))
    try:
        for table in _TABLES:
            con.execute(f"DROP TABLE IF EXISTS {table}")
            log.info(f"  dropped {table}")
    finally:
        con.close()
    log.info("Warehouse cleaned.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    _root = Path(__file__).resolve().parent.parent
    _db   = _root / "warehouse.duckdb"
    clean_history(_db)
    sys.exit(0)