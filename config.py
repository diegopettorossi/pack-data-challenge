"""
config.py — Pipeline configuration.

The canonical source of truth is config.yaml — edit that, not this file.
PipelineConfig.from_yaml() loads it at startup; Python field defaults serve as
fallbacks when the YAML is absent (clean clone, CI) so the pipeline always runs.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    import yaml
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

log = logging.getLogger(__name__)

# Default config.yaml location: same directory as this file.
_DEFAULT_YAML = Path(__file__).resolve().parent / "config.yaml"


@dataclass
class PipelineConfig:
    """
    All parameters for one pipeline run.

    Use PipelineConfig.from_yaml() in production. Constructing directly is fine
    for tests and programmatic use; field defaults mirror config.yaml.
    """

    # ── Session modeling ─────────────────────────────────────────────────
    default_session_duration_minutes: int = 30

    # ── Rebooking analysis ───────────────────────────────────────────────
    tiers_group_a: list[str] = field(default_factory=lambda: ["Gold"])
    tiers_group_b: list[str] = field(default_factory=lambda: ["Silver", "Bronze"])
    rebooking_window_days: Optional[int] = 30

    # ── Data quality thresholds ──────────────────────────────────────────
    dq_max_orphan_rate: float = 0.05
    dq_max_duration_minutes: int = 240

    # ── Infrastructure ───────────────────────────────────────────────────
    environment: str = "dev"   # dev | staging | prod
    db_path: str = "warehouse.duckdb"
    known_tiers: list[str] = field(default_factory=lambda: ["Gold", "Silver", "Bronze"])
    # ── Guardrails ─────────────────────────────────────────────
    # Maximum wall-clock seconds allowed for the full pipeline run.
    # The StepTimer in run_pipeline.py raises PipelineTimeoutError if this is
    # exceeded.  Set to 0 to disable the timeout entirely.
    step_timeout_seconds: int = 300
    # ── Class-level factory ──────────────────────────────────────────────
    @classmethod
    def from_yaml(cls, path: Path | str | None = None) -> "PipelineConfig":
        """
        Load config from YAML. Falls back to Python defaults if the file is
        absent, yaml is unavailable, or any individual key is missing.
        """
        if path is None:
            path = _DEFAULT_YAML

        if not _YAML_AVAILABLE:
            log.warning(
                "PyYAML not installed — using Python defaults. "
                "Run `pip install pyyaml` to enable YAML config."
            )
            return cls()

        try:
            with open(path, encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
        except FileNotFoundError:
            log.warning(f"config.yaml not found at {path} — using Python defaults.")
            return cls()

        session  = data.get("session",      {}) or {}
        rebooking = data.get("rebooking",   {}) or {}
        dq        = data.get("data_quality",{}) or {}
        db        = data.get("database",    {}) or {}
        gr        = data.get("guardrails",  {}) or {}

        env = data.get("environment", "dev")
        _env_paths = {
            "dev":     "warehouse_dev.duckdb",
            "staging": "warehouse_staging.duckdb",
            "prod":    "warehouse.duckdb",
        }
        default_db = _env_paths.get(env, "warehouse.duckdb")

        return cls(
            environment           = env,
            default_session_duration_minutes = session.get(
                "default_duration_minutes", 30),
            tiers_group_a         = rebooking.get("tiers_group_a",  ["Gold"]),
            tiers_group_b         = rebooking.get("tiers_group_b",  ["Silver", "Bronze"]),
            rebooking_window_days = rebooking.get("window_days",    30),
            dq_max_orphan_rate    = dq.get("max_orphan_rate",       0.05),
            dq_max_duration_minutes = dq.get("max_duration_minutes", 240),
            db_path               = db.get("path", default_db),
            known_tiers           = data.get("known_tiers",         ["Gold", "Silver", "Bronze"]),
            step_timeout_seconds  = gr.get("step_timeout_seconds",  300),
        )

    def db_absolute_path(self, base_dir: Path) -> Path:
        """Resolve db_path relative to *base_dir* (solution/ root)."""
        p = Path(self.db_path)
        return p if p.is_absolute() else (base_dir / p).resolve()

    # ── Validation ───────────────────────────────────────────────────────
    def validate(self) -> None:
        """Raise ValueError on self-contradictory configuration before the pipeline starts."""
        if not self.tiers_group_a:
            raise ValueError("tiers_group_a must contain at least one tier.")
        if not self.tiers_group_b:
            raise ValueError("tiers_group_b must contain at least one tier.")

        overlap = set(self.tiers_group_a) & set(self.tiers_group_b)
        if overlap:
            raise ValueError(
                f"Tier groups must be mutually exclusive. "
                f"Overlapping tiers: {sorted(overlap)}"
            )

        if self.default_session_duration_minutes <= 0:
            raise ValueError("default_session_duration_minutes must be > 0.")

        if self.rebooking_window_days is not None and self.rebooking_window_days <= 0:
            raise ValueError("rebooking_window_days must be > 0 or None.")

    # ── SQL template helpers ─────────────────────────────────────────────
    # Produce SQL fragments injected into template SQL files.
    # Tier names are sanitised here (not at injection sites) so the logic
    # is tested once and applied consistently.

    @staticmethod
    def _sanitize_tier(tier: str) -> str:
        """Escape single quotes in a tier name to prevent SQL injection."""
        return tier.replace("'", "''")

    def tiers_a_sql(self) -> str:
        """SQL IN-list for group A  →  'Gold'  or  'Gold','Platinum' """
        return ", ".join(f"'{self._sanitize_tier(t)}'" for t in self.tiers_group_a)

    def tiers_b_sql(self) -> str:
        """SQL IN-list for group B  →  'Silver','Bronze' """
        return ", ".join(f"'{self._sanitize_tier(t)}'" for t in self.tiers_group_b)

    def group_a_label(self) -> str:
        """Human-readable label for group A  →  'Gold'  or  'Gold/Platinum' """
        return "/".join(self.tiers_group_a)

    def group_b_label(self) -> str:
        """Human-readable label for group B  →  'Silver/Bronze' """
        return "/".join(self.tiers_group_b)

    def window_condition_sql(self) -> str:
        """SQL boolean added to the rebooking window check in analysis_rebooking.sql.

        Applied as:  AND {WINDOW_CONDITION}  inside the CASE WHEN ... END.
        Aliases used: usr = user_sessions_ranked, fs = first_sessions.
        """
        if self.rebooking_window_days is None:
            return "TRUE"
        return (
            f"usr.started_at <= fs.first_session_at + INTERVAL '{self.rebooking_window_days}' DAY"
        )

    def to_json(self) -> str:
        """Serialise config to JSON for the pipeline run log."""
        return json.dumps({
            "environment":            self.environment,
            "default_session_duration_minutes": self.default_session_duration_minutes,
            "tiers_group_a":          self.tiers_group_a,
            "tiers_group_b":          self.tiers_group_b,
            "rebooking_window_days":  self.rebooking_window_days,
            "dq_max_orphan_rate":     self.dq_max_orphan_rate,
            "dq_max_duration_minutes":self.dq_max_duration_minutes,
            "db_path":                self.db_path,
            "step_timeout_seconds":   self.step_timeout_seconds,
        })
