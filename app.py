"""
app.py — Streamlit dashboard for the Pack Data Challenge pipeline.

Layout (no fixed sidebar — config appears only where relevant):
  Tab 1 – 🚀 Pipeline     : three run buttons + active config info + run log + summary + danger zone
  Tab 2 – 🔁 Rebooking    : inline group/window controls, ▶ Run, chart + CI table + metric definitions
  Tab 3 – 📋 Reliability  : refresh/clear buttons, overall tiles, pivoted tier table + bar chart + metric defs
  Tab 4 – 🕒 History      : paginated run log table + clear button

Session state keys
  results          – latest merged results dict from run_pipeline.main()
  _tiers_a/b       – persisted tier selections shared across tabs
  _window_days     – persisted rebooking window (int | None)
  _duration        – persisted default session duration (int, internal only)
  _analysis_ran    – True once any analysis has completed
  _confirm_clear   – confirmation flag for history wipe
  _confirm_drop    – confirmation flag for warehouse clear
  _hist_page       – current page index for run history
  _last_run_mode   – most recent pipeline mode ('full'|'ingest-only'|'transform-only'|None)
"""

import subprocess
import sys
import json as _json
import logging
import shutil
from pathlib import Path

# ── Bootstrap ─────────────────────────────────────────────────────────────────
for _mod, _pkg in [("duckdb", "duckdb"), ("polars", "polars"), ("yaml", "pyyaml")]:
    try:
        __import__(_mod)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", _pkg])

import polars as pl
import streamlit as st
from streamlit.delta_generator import DeltaGenerator

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))
from config import PipelineConfig

# ── Load defaults once ────────────────────────────────────────────────────────
_defaults = PipelineConfig.from_yaml()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Pack Data Intelligence",
    page_icon="📊",
    layout="wide",
)

# ── Session state initialisation ──────────────────────────────────────────────
for _k, _v in {
    "results":        None,
    "_tiers_a":       _defaults.tiers_group_a,
    "_tiers_b":       _defaults.tiers_group_b,
    "_window_days":   _defaults.rebooking_window_days,
    "_duration":      _defaults.default_session_duration_minutes,
    "_analysis_ran":  False,
    "_confirm_clear": False,
    "_hist_page":     0,
    "_confirm_drop":  False,
    "_last_run_mode": None,
}.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ── Helper: run pipeline and stream logs ──────────────────────────────────────
_MODE_LABELS: dict[str, str] = {
    "full":           "full pipeline",
    "ingest-only":    "ingestion",
    "transform-only": "transform & validation",
    "analysis-only":  "analysis",
}


def _run_mode(mode: str, cfg: PipelineConfig) -> dict | None:
    """Execute one pipeline mode; stream logs into a st.status widget."""
    _label = _MODE_LABELS.get(mode, mode)
    with st.status(f"⏳ Running **{_label}**…", expanded=True) as _status:
        _log_area = st.empty()

        class _StreamHandler(logging.Handler):
            _TAIL = 14

            def __init__(self, container: DeltaGenerator) -> None:
                super().__init__()
                self._c = container
                self._lines: list[str] = []

            def emit(self, record: logging.LogRecord) -> None:
                self._lines.append(self.format(record))
                self._c.code("\n".join(self._lines[-self._TAIL:]), language="bash")

        _h = _StreamHandler(_log_area)
        _h.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        _root = logging.getLogger()
        _root.addHandler(_h)
        try:
            from run_pipeline import main as _run
            results = _run(cfg, mode=mode)
            if results is None:
                _status.update(label="⚠️ Pipeline returned no data.", state="error", expanded=True)
                return None
            _status.update(label=f"✅ {_label.capitalize()} complete!", state="complete", expanded=False)
            return results
        except Exception as exc:
            _err = str(exc)
            if any(kw in _err.lower() for kw in ("lock", "database is locked")):
                _status.update(label="⏳ Warehouse locked — retry in a moment", state="error")
                st.error(
                    "DuckDB write lock is held by another process. "
                    "Wait a moment and retry, or restart Streamlit."
                )
            else:
                _status.update(label=f"❌ {_label.capitalize()} failed", state="error")
                st.error(f"**Error:** {exc}")
            return None
        finally:
            _root.removeHandler(_h)


def _merge(existing: dict | None, new: dict | None) -> dict | None:
    """Merge new results onto existing so prior data isn't wiped by partial runs."""
    if new is None:
        return existing
    if existing is None:
        return new
    return {**existing, **new}


# ── Read persisted state (values from PREVIOUS Streamlit run) ─────────────────
_ss_tiers_a  = st.session_state["_tiers_a"]
_ss_tiers_b  = st.session_state["_tiers_b"]
_ss_window   = st.session_state["_window_days"]
_ss_duration = st.session_state["_duration"]

_overlap = set(_ss_tiers_a) & set(_ss_tiers_b)
_cfg_ok  = bool(_ss_tiers_a and _ss_tiers_b and not _overlap)
_active_cfg = (
    PipelineConfig(
        default_session_duration_minutes=_ss_duration,
        tiers_group_a=_ss_tiers_a,
        tiers_group_b=_ss_tiers_b,
        rebooking_window_days=_ss_window,
    )
    if _cfg_ok else _defaults
)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📊 Pack Data Challenge — Mentor Impact Analysis")
st.caption(
    "End-to-end analytics platform: ingest raw booking data, run dbt transformations, "
    "and explore rebooking rates & booking reliability by mentor tier."
)

# ── Tab navigation ────────────────────────────────────────────────────────────
tab_pipeline, tab_rebooking, tab_reliability, tab_history = st.tabs([
    "🚀  Pipeline",
    "🔁  Rebooking Rate",
    "📋  Booking Reliability",
    "🕒  Run History",
])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 – PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════
with tab_pipeline:

    st.subheader("🚀 Run the pipeline")
    st.caption(
        "Execute one or more pipeline stages: load raw data into DuckDB, rebuild "
        "dimension & fact tables with dbt, validate data quality, and produce analysis results."
    )

    # ── Active config status (collapsed) ──────────────────────────────────────────
    with st.expander("ℹ️ Active configuration", expanded=False):
        if _cfg_ok:
            _wl = f"{_ss_window}-day rebooking window" if _ss_window else "no time limit"
            st.success(
                f"Groups: **{_active_cfg.group_a_label()}** vs "
                f"**{_active_cfg.group_b_label()}** · {_wl}"
            )
        else:
            st.warning(
                "Tier group configuration is invalid — "
                "set it on the **🔁 Rebooking Rate** tab."
            )
        st.caption(
            "Tier groups and rebooking window are configured on the **🔁 Rebooking Rate** tab "
            "and shared across all pipeline modes."
        )

    st.markdown("---")

    # ── Action buttons — inline single row ─────────────────────────────────────
    _bc1, _bc2, _bc3 = st.columns(3)

    with _bc1:
        _btn_full = st.button(
            "▶ Full pipeline",
            key="btn_full", type="primary", width="stretch",
            disabled=not _cfg_ok,
            help="Ingest → dbt snapshot/run → validate → analyse",
        )
    with _bc2:
        _btn_ingest = st.button(
            "📥 Ingest only",
            key="btn_ingest", width="stretch",
            help="Load raw CSV + JSON into DuckDB",
        )
    with _bc3:
        _btn_transform = st.button(
            "⚙️ Transform & validate",
            key="btn_transform", width="stretch",
            help="dbt snapshot + run + test (assumes raw data already loaded)",
        )

    st.markdown("---")

    # ── Execute ───────────────────────────────────────────────────────────────
    if _btn_full:
        _r = _run_mode("full", _active_cfg)
        st.session_state.results = _merge(st.session_state.results, _r)
        if _r:
            st.session_state["_analysis_ran"] = True
        st.session_state["_last_run_mode"] = "full"

    elif _btn_ingest:
        _r = _run_mode("ingest-only", _active_cfg)
        st.session_state.results = _merge(st.session_state.results, _r)
        st.session_state["_last_run_mode"] = "ingest-only"

    elif _btn_transform:
        _r = _run_mode("transform-only", _active_cfg)
        st.session_state.results = _merge(st.session_state.results, _r)
        st.session_state["_last_run_mode"] = "transform-only"

    # ── Clear warehouse (danger zone) ───────────────────────────────────────────────
    with st.expander("⚠️ Danger zone", expanded=False):
        st.caption(
            "Delete all rows from every table in the local DuckDB warehouse. "
            "Use before a clean re-ingest to start from a blank state."
        )
        if st.button(
            "🗑️  Clear all warehouse tables",
            key="btn_drop_warehouse",
            help="Runs DELETE FROM on every base table in warehouse.duckdb",
        ):
            st.session_state["_confirm_drop"] = True

        if st.session_state.get("_confirm_drop"):
            st.warning(
                "🚨 This will permanently delete **all rows** in the warehouse. "
                "The pipeline will need a full re-run afterwards."
            )
            _dc1, _dc2, _ = st.columns([1, 1, 5])
            if _dc1.button("Yes, clear", type="primary", key="btn_drop_confirm"):
                import duckdb as _ddb
                _wcon = _ddb.connect(str(BASE_DIR / "warehouse.duckdb"))
                try:
                    _tables = [
                        r[0] for r in _wcon.execute(
                            "SELECT table_name FROM information_schema.tables "
                            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
                        ).fetchall()
                    ]
                    for _t in _tables:
                        try:
                            _wcon.execute(f'DELETE FROM "{_t}"')
                        except Exception:
                            pass
                    _wcon.close()
                    st.session_state["_confirm_drop"] = False
                    st.session_state.results = None
                    st.success(f"Cleared {len(_tables)} table(s): {', '.join(_tables)}")
                    st.rerun()
                except Exception as _e:
                    _wcon.close()
                    st.error(f"Failed to clear warehouse: {_e}")
            if _dc2.button("Cancel", key="btn_drop_cancel"):
                st.session_state["_confirm_drop"] = False
                st.rerun()

    # ── Summary ───────────────────────────────────────────────────────────────
    _res       = st.session_state.results
    _last_mode = st.session_state.get("_last_run_mode")
    if _res:
        _rc: PipelineConfig = _res.get("config", _active_cfg)
        if _last_mode == "transform-only":
            st.success("Data quality checks passed.")
        elif _last_mode == "ingest-only":
            _m2, _m3 = st.columns(2)
            _m2.metric("New users ingested",  _res.get("new_users",  0))
            _m3.metric("New events ingested", _res.get("new_events", 0))
        else:  # full or not yet set
            _m1, _m2, _m3, _m4 = st.columns(4)
            _m1.metric("Run ID",               f"#{_res.get('run_id', '—')}")
            _m2.metric("New users ingested",   _res.get("new_users",  0))
            _m3.metric("New events ingested",  _res.get("new_events", 0))
            _w = _rc.rebooking_window_days
            _m4.metric("Rebooking window", f"{_w} days" if _w else "Unlimited")

        if _res.get("rebooking_df") is not None and _last_mode != "transform-only":
            st.success(
                "Analysis results ready — see **🔁 Rebooking Rate** and **📋 Booking Reliability** tabs."
            )
    else:
        st.info("No run yet. Click one of the buttons above to start.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 – REBOOKING RATE
# ═══════════════════════════════════════════════════════════════════════════════
with tab_rebooking:

    st.subheader("🔁 Rebooking Rate by Tier")
    st.caption(
        "Compare what share of users booked a follow-up session across two mentor tier groups. "
        "Select which tiers form each group, optionally restrict to a post-first-session time window, "
        "then click ▶ Run."
    )

    # ── Metric definitions (always visible, above controls) ────────────────────
    with st.expander("📖 Metric definitions"):
        st.markdown("""
| Metric | Definition |
|---|---|
| **Total Users** | Distinct users who had at least one session with a mentor in this group |
| **Rebooked** | Users who booked a second session with the same tier group |
| **Rebooking Rate (%)** | Rebooked ÷ Total Users × 100 |
| **95% CI Lower / Upper** | Wilson score confidence interval for the rebooking proportion |
| **Rebooking window** | Only sessions within N days of the user’s first session count towards rebooking |
""")

    st.divider()

    # ── Row 1: group selectors + run button ─────────────────────────────────
    _rc1, _rc2, _rc4 = st.columns([3, 3, 2])

    with _rc1:
        _ta = st.multiselect(
            "Group A — Target",
            options=_defaults.known_tiers,
            default=_ss_tiers_a,
            key="rb_tiers_a",
            help="Mentor tiers assigned to the experimental / target group.",
        )
    with _rc2:
        _tb = st.multiselect(
            "Group B — Control",
            options=_defaults.known_tiers,
            default=_ss_tiers_b,
            key="rb_tiers_b",
            help="Mentor tiers assigned to the control group.",
        )
    with _rc4:
        st.markdown("&nbsp;", unsafe_allow_html=True)
        _btn_rb = st.button(
            "▶  Run",
            key="btn_run_rb",
            type="primary",
            width="stretch",
            disabled=not bool(_ta and _tb and not (set(_ta) & set(_tb))),
            help="Run rebooking rate analysis with the current parameters",
        )

    # ── Row 2: rebooking window (aligned under Group A) ────────────────────────
    _rw1, _rw2 = st.columns([3, 5])
    with _rw1:
        _uw = st.toggle(
            "Rebooking window",
            value=(_ss_window is not None),
            key="rb_use_window",
            help="Restrict rebooking count to sessions within N days of the user's first session.",
        )
        if _uw:
            _wi: int | None = st.number_input(
                "Days after first session",
                min_value=7, max_value=365,
                value=int(_ss_window) if _ss_window else 30,
                step=7, key="rb_window_days",
            )
        else:
            _wi = None

    # Persist back to session state for cross-tab sharing
    _ov_rb  = set(_ta) & set(_tb)
    _rb_ok  = bool(_ta and _tb and not _ov_rb)

    if _ta  != _ss_tiers_a: st.session_state["_tiers_a"]    = _ta
    if _tb  != _ss_tiers_b: st.session_state["_tiers_b"]    = _tb
    if _wi  != _ss_window:  st.session_state["_window_days"] = _wi

    _rb_cfg = (
        PipelineConfig(
            default_session_duration_minutes=_ss_duration,
            tiers_group_a=_ta,
            tiers_group_b=_tb,
            rebooking_window_days=_wi,
        )
        if _rb_ok else _defaults
    )

    # Validation feedback (compact, below the controls row)
    if not _rb_ok:
        if not _ta:  st.caption("⚠️ Group A is empty.")
        if not _tb:  st.caption("⚠️ Group B is empty.")
        if _ov_rb:   st.caption(f"⚠️ Groups overlap: {', '.join(sorted(_ov_rb))}.")

    if _btn_rb and _rb_ok:
        _r = _run_mode("analysis-only", _rb_cfg)
        st.session_state.results = _merge(st.session_state.results, _r)
        if _r:
            st.session_state["_analysis_ran"] = True
            st.rerun()

    # ── Results display ───────────────────────────────────────────────────────
    _rb_res = st.session_state.results
    if _rb_res and _rb_res.get("rebooking_df") is not None:
        st.divider()
        _rc_rb: PipelineConfig = _rb_res.get("config", _active_cfg)
        _rb_df   = _rb_res["rebooking_df"]
        _ci_rows = _rb_res.get("ci_rows", [])
        _ci_df   = pl.DataFrame(_ci_rows)

        _wl_r = (
            f"{_rc_rb.rebooking_window_days}-day rebooking window"
            if _rc_rb.rebooking_window_days else "no time limit"
        )
        st.caption(
            f"Results for **{_rc_rb.group_a_label()}** vs **{_rc_rb.group_b_label()}** · {_wl_r}"
        )

        _rb_chart, _rb_tbl = st.columns([3, 2])
        with _rb_chart:
            _chart_df = _rb_df.rename({"rebooking_rate_pct": "Rebooking Rate (%)", "mentor_tier": "Mentor Group"})
            st.bar_chart(_chart_df, x="Mentor Group", y="Rebooking Rate (%)")
        with _rb_tbl:
            st.dataframe(
                _ci_df.rename({
                    "mentor_tier":        "Tier",
                    "total_users":        "Total Users",
                    "users_rebooked":     "Rebooked",
                    "rebooking_rate_pct": "Rate (%)",
                    "ci_lower_pct":       "95% CI Lower",
                    "ci_upper_pct":       "95% CI Upper",
                }).select(["Tier", "Total Users", "Rebooked", "Rate (%)", "95% CI Lower", "95% CI Upper"]),
                width="stretch",
                hide_index=True,
            )

        _MIN_N = 30
        for _row in _ci_df.filter(pl.col("total_users") < _MIN_N).iter_rows(named=True):
            st.warning(
                f"⚠️ **{_row['mentor_tier']}** has only **{_row['total_users']} users** — "
                f"treat as directional only (recommended minimum: {_MIN_N} per group)."
            )
    else:
        st.info(
            "Configure the parameters above and click **▶ Run** to start the analysis, "
            "or run the full pipeline from the **🚀 Pipeline** tab."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 – BOOKING RELIABILITY
# ═══════════════════════════════════════════════════════════════════════════════
with tab_reliability:

    # ── Header row: title + controls ──────────────────────────────────────────
    _br_hdr, _br_ctrl1, _br_ctrl2 = st.columns([5, 1, 1])
    _br_hdr.subheader("📋 Booking Reliability by Tier")
    _br_hdr.caption(
        "Understand how bookings resolve across mentor tiers: confirmation, cancellation, "
        "and no-show rates, with an overall summary and a per-tier breakdown."
    )
    with _br_ctrl1:
        _btn_br = st.button(
            "🔄 Refresh",
            key="btn_run_br",
            width="stretch",
            help="Re-run analysis and reload booking reliability data",
        )
    with _br_ctrl2:
        _btn_br_clear = st.button(
            "🗑️ Clear",
            key="btn_clear_br",
            width="stretch",
            help="Remove reliability results from the current session",
        )

    if _btn_br:
        _r = _run_mode("analysis-only", _active_cfg if _cfg_ok else _defaults)
        st.session_state.results = _merge(st.session_state.results, _r)
        if _r:
            st.session_state["_analysis_ran"] = True
            st.rerun()

    if _btn_br_clear and st.session_state.results:
        st.session_state.results.pop("booking_reliability_df", None)
        st.rerun()

    # ── Metric definitions (always visible) ─────────────────────────────────
    with st.expander("📖 Metric definitions"):
        st.markdown("""
| Metric | Definition |
|---|---|
| **Total Bookings** | All booking requests regardless of outcome |
| **Confirmed** | Bookings that were accepted and took place |
| **Cancelled** | Bookings cancelled by either party before the session |
| **No-shows** | Confirmed bookings where the user did not attend |
| **Pending / Orphans** | Bookings not yet resolved or missing outcome events |
| **Confirmation Rate (%)** | Confirmed ÷ Total Bookings × 100 |
| **No-show Rate (%)** | No-shows ÷ Confirmed × 100 |
| **Cancellation Rate (%)** | Cancelled ÷ Total Bookings × 100 |
""")

    # ── Results display ───────────────────────────────────────────────────────
    _br_res = st.session_state.results
    _br_df  = _br_res.get("booking_reliability_df") if _br_res else None

    if _br_df is not None:
        st.divider()

        # Overall summary — no delta arrows
        _tot  = int(_br_df["total_bookings"].sum())
        _conf = int(_br_df["confirmed_count"].sum())
        _canc = int(_br_df["cancelled_count"].sum())
        _ns   = int(_br_df["no_show_count"].sum())
        _pend = int(_br_df["pending_count"].sum())

        st.markdown("**Overall**")
        _ov1, _ov2, _ov3, _ov4, _ov5 = st.columns(5)
        _ov1.metric("Total Bookings",    f"{_tot:,}")
        _ov2.metric("Confirmed",         f"{_conf:,} ({100*_conf/_tot:.1f}%)" if _tot else f"{_conf:,}")
        _ov3.metric("Cancelled",         f"{_canc:,} ({100*_canc/_tot:.1f}%)" if _tot else f"{_canc:,}")
        _ov4.metric("No-shows",          f"{_ns:,} ({100*_ns/max(_conf,1):.1f}% of confirmed)")
        _ov5.metric("Pending / Orphans", f"{_pend:,} ({100*_pend/_tot:.1f}%)" if _tot else f"{_pend:,}")

        st.divider()
        st.markdown("**Breakdown by tier**")

        # Pivoted table: metrics as rows, tiers as columns
        _pivot_metrics = [
            ("Total Bookings",           "total_bookings"),
            ("Confirmed",                "confirmed_count"),
            ("Cancelled",                "cancelled_count"),
            ("No-shows",                 "no_show_count"),
            ("Pending",                  "pending_count"),
            ("Confirmation Rate (%)",    "confirmation_rate_pct"),
            ("No-show Rate (%)",         "no_show_rate_pct"),
            ("Cancellation Rate (%)",    "cancellation_rate_pct"),
        ]
        _pivot_rows: list[dict] = []
        for _lbl, _col in _pivot_metrics:
            if _col in _br_df.columns:
                _row: dict = {"Metric": _lbl}
                for _tr in _br_df.iter_rows(named=True):
                    _v = _tr[_col]
                    _row[_tr["mentor_tier"]] = (
                        f"{_v:,.1f}" if isinstance(_v, float) else f"{_v:,}"
                    )
                _pivot_rows.append(_row)

        if _pivot_rows:
            st.dataframe(
                pl.DataFrame(_pivot_rows),
                width="stretch",
                hide_index=True,
            )

        # Bar chart (rates only)
        _rate_cols = [c for c in ["confirmation_rate_pct", "no_show_rate_pct", "cancellation_rate_pct"] if c in _br_df.columns]
        if _rate_cols:
            _br_chart_df = _br_df.rename({
                "mentor_tier":           "Mentor Tier",
                "confirmation_rate_pct": "Confirmed (%)",
                "no_show_rate_pct":      "No-show (%)",
                "cancellation_rate_pct": "Cancelled (%)",
            })
            _renamed_rate_cols = [c for c in ["Confirmed (%)", "No-show (%)", "Cancelled (%)"] if c in _br_chart_df.columns]
            st.divider()
            st.markdown("**Rate comparison by tier**")
            st.bar_chart(_br_chart_df, x="Mentor Tier", y=_renamed_rate_cols)

    else:
        st.info(
            "Click **🔄 Refresh** above to load booking reliability data, "
            "or run the full pipeline from the **🚀 Pipeline** tab."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 – RUN HISTORY
# ═══════════════════════════════════════════════════════════════════════════════
with tab_history:

    _hist_hdr_col, _hist_btn_col = st.columns([6, 1])
    _hist_hdr_col.subheader("🕒 Run History")
    _hist_hdr_col.caption(
        "A log of every pipeline execution — status, duration, configuration used, "
        "and data quality outcome."
    )

    _logs_dir = BASE_DIR / "logs"

    with _hist_btn_col:
        if st.button("🗑️ Clear", key="btn_clear_hist",
                     width="stretch", help="Delete all run log files."):
            st.session_state["_confirm_clear"] = True

    if st.session_state.get("_confirm_clear"):
        st.warning("This will permanently delete all run history. Are you sure?")
        _yc, _nc, _ = st.columns([1, 1, 5])
        if _yc.button("Yes, delete", type="primary", key="btn_confirm_del"):
            if _logs_dir.exists():
                shutil.rmtree(_logs_dir)
            st.session_state["_confirm_clear"] = False
            st.success("Run history cleared.")
            st.rerun()
        if _nc.button("Cancel", key="btn_cancel_del"):
            st.session_state["_confirm_clear"] = False
            st.rerun()

    if _logs_dir.exists():
        _rows: list[dict] = []
        for _f in sorted(_logs_dir.rglob("*.json"), reverse=True):
            try:
                for _d in _json.loads(_f.read_text(encoding="utf-8")):
                    _cfg = _d.get("config") or {}
                    _a   = _cfg.get("tiers_group_a", [])
                    _b   = _cfg.get("tiers_group_b", [])
                    _rows.append({
                        "run_id":      _d.get("run_id"),
                        "started_at":  _d.get("started_at"),
                        "status":      _d.get("status"),
                        "duration_s":  _d.get("duration_seconds"),
                        "group_a":     "/".join(_a) if _a else "—",
                        "group_b":     "/".join(_b) if _b else "—",
                        "window_days": _cfg.get("rebooking_window_days", "—"),
                        "new_users":   _d.get("new_users_ingested"),
                        "dq_passed":   _d.get("dq_checks_passed"),
                        "warnings":    len(_d.get("warnings", [])),
                    })
            except Exception:
                pass

        if _rows:
            _rows = sorted(_rows, key=lambda r: r.get("started_at") or "", reverse=True)
            _PAGE = 20
            _total_pages = max(1, (len(_rows) + _PAGE - 1) // _PAGE)
            _page = st.session_state.get("_hist_page", 0)
            _page = max(0, min(_page, _total_pages - 1))

            if _total_pages > 1:
                _ph_col, _pn_col, _pp_col = st.columns([4, 1, 1])
                _ph_col.caption(f"Page {_page + 1} of {_total_pages} ({len(_rows)} total runs)")
                if _pn_col.button("◀ Prev", key="btn_hist_prev", disabled=(_page == 0)):
                    st.session_state["_hist_page"] = _page - 1
                    st.rerun()
                if _pp_col.button("Next ▶", key="btn_hist_next", disabled=(_page >= _total_pages - 1)):
                    st.session_state["_hist_page"] = _page + 1
                    st.rerun()
            else:
                st.caption(f"{len(_rows)} run{'s' if len(_rows) != 1 else ''}")

            _page_rows = _rows[_page * _PAGE : (_page + 1) * _PAGE]
            st.dataframe(
                pl.DataFrame(_page_rows),
                width="stretch",
                hide_index=True,
                column_config={
                    "group_a":     st.column_config.TextColumn("Group A"),
                    "group_b":     st.column_config.TextColumn("Group B"),
                    "window_days": st.column_config.TextColumn("Window (days)"),
                    "dq_passed":   st.column_config.CheckboxColumn("DQ ✓"),
                    "warnings":    st.column_config.NumberColumn("Warnings"),
                },
            )
        else:
            st.info("No run logs found yet.")
    else:
        st.info("No run logs directory found. Run the pipeline to create one.")
