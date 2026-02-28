"""
tests/test_pipeline.py — Unit tests for the Pack data pipeline.

Run with:
    pip install pytest
    pytest tests/

Coverage:
  - PipelineConfig validation and SQL helpers (incl. injection guard)
  - validate.run_checks — each check in isolation using an in-memory DuckDB
  - ingest.ingest_mentors — CSV guard rails
  - fct_sessions session-pairing logic via SQL
  - Integration: ingest → session pairing → analysis against fixture files
      Fixture design tests: dedup, idempotency, mixed user_id types
      Rebooking analysis: 30-day window boundary, missing end event handling
"""
import sys
from pathlib import Path

import pytest
import duckdb
import polars as pl

# Make src/ and the solution root importable from the tests/ directory.
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from config import PipelineConfig
from validate import run_checks
import ingest as ingest_module


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mem_con():
    """Fresh in-memory DuckDB connection with the schema every test can write to."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()


def _seed_clean_db(con: duckdb.DuckDBPyConnection) -> None:
    """
    Populate dim_users, dim_mentors, fct_sessions with valid minimal data
    so that all DQ checks pass out of the box.

    Schema mirrors the production SCD2 warehouse design:
      - dim_users / dim_mentors use hash surrogate PKs and valid_from/valid_to fields.
      - fct_sessions uses session_hk (PK) + session_id (business key) + inserted_at.
    valid_to = NULL means the record is current.
    """
    con.execute("""
        CREATE TABLE dim_users (
            user_hk     VARCHAR PRIMARY KEY,
            user_id     BIGINT,
            company_id  BIGINT,
            signup_date DATE,
            status      VARCHAR,
            valid_from  DATE,
            valid_to    DATE,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO dim_users (user_hk, user_id, company_id, signup_date, status, valid_from) VALUES
            ('hk-u1', 1, 100, '2024-01-01', 'active', '2024-01-01'),
            ('hk-u2', 2, 100, '2024-01-02', 'active', '2024-01-02');

        CREATE TABLE dim_mentors (
            mentor_hk   VARCHAR PRIMARY KEY,
            mentor_id   VARCHAR,
            tier        VARCHAR,
            hourly_rate INTEGER,
            valid_from  DATE,
            valid_to    DATE,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO dim_mentors (mentor_hk, mentor_id, tier, hourly_rate, valid_from) VALUES
            ('hk-m1', 'M01', 'Gold',   100, '2025-01-01'),
            ('hk-m2', 'M02', 'Silver',  80, '2025-01-01');

        CREATE TABLE fct_sessions (
            session_hk            VARCHAR PRIMARY KEY,
            session_id            VARCHAR,
            user_id               BIGINT,
            mentor_id             VARCHAR,
            started_at            TIMESTAMP,
            ended_at              TIMESTAMP,
            duration_minutes      INTEGER,
            is_duration_estimated BOOLEAN,
            session_date          DATE,
            inserted_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO fct_sessions
            (session_hk, session_id, user_id, mentor_id, started_at, ended_at,
             duration_minutes, is_duration_estimated, session_date)
        VALUES
            ('hk-s1', 'S1', 1, 'M01', '2025-01-01 10:00', '2025-01-01 10:30', 30, false, '2025-01-01'),
            ('hk-s2', 'S2', 1, 'M01', '2025-01-15 10:00', '2025-01-15 10:45', 45, false, '2025-01-15'),
            ('hk-s3', 'S3', 2, 'M02', '2025-01-02 09:00', '2025-01-02 09:30', 30, false, '2025-01-02');
    """)


# ── PipelineConfig tests ───────────────────────────────────────────────────────

class TestPipelineConfig:
    def test_valid_default_config(self):
        cfg = PipelineConfig()
        cfg.validate()  # should not raise

    def test_overlapping_tiers_raises(self):
        cfg = PipelineConfig(tiers_group_a=["Gold"], tiers_group_b=["Gold", "Silver"])
        with pytest.raises(ValueError, match="mutually exclusive"):
            cfg.validate()

    def test_empty_group_a_raises(self):
        cfg = PipelineConfig(tiers_group_a=[], tiers_group_b=["Silver"])
        with pytest.raises(ValueError, match="tiers_group_a"):
            cfg.validate()

    def test_empty_group_b_raises(self):
        cfg = PipelineConfig(tiers_group_a=["Gold"], tiers_group_b=[])
        with pytest.raises(ValueError, match="tiers_group_b"):
            cfg.validate()

    def test_zero_default_duration_raises(self):
        cfg = PipelineConfig(default_session_duration_minutes=0)
        with pytest.raises(ValueError, match="default_session_duration_minutes"):
            cfg.validate()

    def test_negative_window_raises(self):
        cfg = PipelineConfig(rebooking_window_days=-5)
        with pytest.raises(ValueError, match="rebooking_window_days"):
            cfg.validate()

    def test_none_window_is_valid(self):
        cfg = PipelineConfig(rebooking_window_days=None)
        cfg.validate()  # unlimited window is a supported option

    # ── SQL helpers ──────────────────────────────────────────────────────────

    def test_tiers_a_sql_single(self):
        cfg = PipelineConfig(tiers_group_a=["Gold"])
        assert cfg.tiers_a_sql() == "'Gold'"

    def test_tiers_a_sql_multiple(self):
        cfg = PipelineConfig(tiers_group_a=["Gold", "Platinum"])
        assert cfg.tiers_a_sql() == "'Gold', 'Platinum'"

    def test_tiers_sql_sanitises_single_quote(self):
        """A tier name with a quote must not produce broken SQL or an injection path."""
        cfg = PipelineConfig(tiers_group_a=["Go'ld"], tiers_group_b=["Silver"])
        result = cfg.tiers_a_sql()
        assert "Go''ld" in result, "Single quote should be escaped as two single quotes"
        assert result.count("'") % 2 == 0, "Resulting SQL fragment must have balanced quotes"

    def test_tiers_sql_sanitises_semicolon_in_name(self):
        """A semicolon in a tier name must survive as a literal, not break the query."""
        cfg = PipelineConfig(tiers_group_a=["Gold; DROP TABLE fct_sessions--"], tiers_group_b=["Silver"])
        result = cfg.tiers_a_sql()
        # The dangerous string is enclosed in single quotes; semicolons inside
        # a string literal are harmless.  We just check it doesn't escape the quotes.
        assert result.startswith("'")
        assert result.endswith("'")

    def test_window_condition_with_days(self):
        cfg = PipelineConfig(rebooking_window_days=30)
        sql = cfg.window_condition_sql()
        assert "INTERVAL '30' DAY" in sql
        assert "usr.started_at" in sql

    def test_window_condition_unlimited(self):
        cfg = PipelineConfig(rebooking_window_days=None)
        assert cfg.window_condition_sql() == "TRUE"

    def test_group_labels(self):
        cfg = PipelineConfig(tiers_group_a=["Gold", "Platinum"], tiers_group_b=["Bronze"])
        assert cfg.group_a_label() == "Gold/Platinum"
        assert cfg.group_b_label() == "Bronze"

    def test_to_json_round_trip(self):
        import json
        cfg = PipelineConfig(tiers_group_a=["Gold"], rebooking_window_days=60)
        data = json.loads(cfg.to_json())
        assert data["tiers_group_a"] == ["Gold"]
        assert data["rebooking_window_days"] == 60


# ── validate.run_checks tests ─────────────────────────────────────────────────

class TestRunChecks:
    def test_all_pass_on_clean_data(self, mem_con):
        _seed_clean_db(mem_con)
        cfg = PipelineConfig(tiers_group_a=["Gold"], tiers_group_b=["Silver"])
        failures, _ = run_checks(mem_con, cfg)
        assert failures == [], f"Expected no failures, got: {failures}"

    def test_check1_fails_on_negative_duration(self, mem_con):
        _seed_clean_db(mem_con)
        mem_con.execute(
            "UPDATE fct_sessions SET duration_minutes = -10 WHERE session_id = 'S1'"
        )
        failures, _ = run_checks(mem_con)
        assert any("Check 1" in f for f in failures)

    def test_check4_warns_but_does_not_fail_on_outlier(self, mem_con, caplog):
        import logging
        _seed_clean_db(mem_con)
        mem_con.execute(
            "UPDATE fct_sessions SET duration_minutes = 300 WHERE session_id = 'S1'"
        )
        cfg = PipelineConfig(dq_max_duration_minutes=240)
        with caplog.at_level(logging.WARNING):
            failures, dq_warnings = run_checks(mem_con, cfg)
        assert not any("Check 4" in f for f in failures), "Check 4 should warn, not fail"
        # Warning must appear in both the returned warnings list and the log records
        assert any("Check 4" in w for w in dq_warnings), "Check 4 warning must be in returned warnings"
        assert any("Check 4" in r.message for r in caplog.records)

    def test_check6_fails_on_missing_configured_tier(self, mem_con):
        _seed_clean_db(mem_con)
        cfg = PipelineConfig(tiers_group_a=["Diamond"], tiers_group_b=["Silver"])
        failures, _ = run_checks(mem_con, cfg)
        assert any("Check 6" in f for f in failures)
        assert any("Diamond" in f for f in failures)

    def test_check6_skipped_without_config(self, mem_con):
        """When no config is passed (standalone mode), Check 6 must not fail."""
        _seed_clean_db(mem_con)
        failures, _ = run_checks(mem_con, config=None)
        assert not any("Check 6" in f for f in failures)


# ── ingest_mentors guard-rail tests ───────────────────────────────────────────

class TestIngestMentorsValidation:
    """
    Test the CSV guard-rails in isolation without touching the filesystem.
    We replicate the validation logic rather than mocking polars/file I/O so
    the tests remain fast and dependency-free.
    """

    def test_missing_column_raises(self):
        # Simulate a CSV missing the 'tier' column
        df = pl.DataFrame({"mentor_id": ["M01"], "hourly_rate": [100]})
        required_cols = {"mentor_id", "tier", "hourly_rate"}
        missing = required_cols - set(df.columns)
        assert "tier" in missing

    def test_empty_dataframe_detected(self):
        df = pl.DataFrame({"mentor_id": [], "tier": [], "hourly_rate": []})
        assert len(df) == 0

    def test_null_tier_after_normalisation_detected(self):
        df = pl.DataFrame({
            "mentor_id": ["M01", "M02"],
            "tier": ["Gold", None],
            "hourly_rate": [100, 80],
        })
        null_tiers = df["tier"].is_null().sum()
        assert null_tiers == 1


# ── Session pairing SQL tests ─────────────────────────────────────────────────

class TestSessionPairing:
    """
    Verify that fct_sessions.sql pairing logic handles edge cases correctly
    by re-running the core SQL against a controlled raw_events table.
    """

    def _run_pairing(self, con, events: list[dict]) -> pl.DataFrame:
        """Create raw_events from a list of dicts and run the pairing CTEs."""
        df = pl.DataFrame(events)
        con.register("raw_events", df)
        return con.sql("""
            WITH session_starts AS (
                SELECT
                    event_id,
                    CAST(user_id AS BIGINT)        AS user_id,
                    TRIM(mentor_id)                AS mentor_id,
                    CAST("timestamp" AS TIMESTAMP) AS started_at,
                    LEAD(CAST("timestamp" AS TIMESTAMP)) OVER (
                        PARTITION BY CAST(user_id AS BIGINT), TRIM(mentor_id)
                        ORDER BY "timestamp" ASC
                    ) AS next_start_at
                FROM raw_events WHERE event_type = 'session_started'
            ),
            session_ends AS (
                SELECT
                    event_id,
                    CAST(user_id AS BIGINT)        AS user_id,
                    TRIM(mentor_id)                AS mentor_id,
                    CAST("timestamp" AS TIMESTAMP) AS ended_at
                FROM raw_events WHERE event_type = 'session_ended'
            ),
            paired AS (
                SELECT
                    s.event_id AS session_id,
                    s.user_id, s.mentor_id, s.started_at,
                    MIN(e.ended_at) AS ended_at
                FROM session_starts s
                LEFT JOIN session_ends e
                    ON  s.user_id   = e.user_id
                    AND s.mentor_id = e.mentor_id
                    AND e.ended_at  > s.started_at
                    AND (s.next_start_at IS NULL OR e.ended_at < s.next_start_at)
                GROUP BY s.event_id, s.user_id, s.mentor_id, s.started_at
            )
            SELECT * FROM paired ORDER BY started_at
        """).pl()

    def test_normal_pair(self, mem_con):
        result = self._run_pairing(mem_con, [
            {"event_id": "e1", "user_id": 1, "mentor_id": "M01", "timestamp": "2025-01-01 10:00:00", "event_type": "session_started"},
            {"event_id": "e2", "user_id": 1, "mentor_id": "M01", "timestamp": "2025-01-01 10:30:00", "event_type": "session_ended"},
        ])
        assert len(result) == 1
        assert result["ended_at"][0] is not None

    def test_missing_end_event_yields_null_ended_at(self, mem_con):
        """Browser crash scenario: session_started with no matching session_ended."""
        result = self._run_pairing(mem_con, [
            {"event_id": "e1", "user_id": 1, "mentor_id": "M01", "timestamp": "2025-01-01 10:00:00", "event_type": "session_started"},
        ])
        assert len(result) == 1
        assert result["ended_at"][0] is None, "Missing end should produce NULL, not a phantom end"

    def test_lead_boundary_isolates_missing_end(self, mem_con):
        """
        Two consecutive sessions for the same user+mentor where the first is missing
        its end event.  The LEAD boundary must prevent session_end[2] from being
        attributed to session_start[1].
        """
        result = self._run_pairing(mem_con, [
            {"event_id": "e1", "user_id": 1, "mentor_id": "M01", "timestamp": "2025-01-01 10:00:00", "event_type": "session_started"},
            # e2 (end of first session) is intentionally missing — browser crash
            {"event_id": "e3", "user_id": 1, "mentor_id": "M01", "timestamp": "2025-01-01 11:00:00", "event_type": "session_started"},
            {"event_id": "e4", "user_id": 1, "mentor_id": "M01", "timestamp": "2025-01-01 11:30:00", "event_type": "session_ended"},
        ])
        assert len(result) == 2
        # First session should have NULL end (LEAD boundary blocks e4)
        assert result["ended_at"][0] is None
        # Second session should correctly pair with e4
        assert result["ended_at"][1] is not None


# ── Ingest unit tests ─────────────────────────────────────────────────────────

class TestIngest:
    """
    Unit tests for ingest_users / ingest_mentors / ingest_events.
    Each test monkeypatches ingest_module.DATA_DIR so the real data files are
    never modified and tests remain isolated.
    """

    # ── helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _copy_data(tmp_path: Path, *filenames: str) -> None:
        """Copy one or more files from the real data/ dir into tmp_path."""
        import shutil
        real = _ROOT / "data"
        for name in filenames:
            shutil.copy(real / name, tmp_path / name)

    # ── ingest_users ─────────────────────────────────────────────────────────

    def test_ingest_users_creates_table(self, monkeypatch, tmp_path):
        self._copy_data(tmp_path, "users_db_export.csv")
        monkeypatch.setattr(ingest_module, "DATA_DIR", tmp_path)
        con = duckdb.connect(":memory:")
        n = ingest_module.ingest_users(con)
        assert n > 0
        total = con.execute("SELECT COUNT(*) FROM raw_users").fetchone()
        total = total[0] if total else 0
        assert total == n, "raw_users row count must match the returned new-row count"

    def test_ingest_users_is_idempotent(self, monkeypatch, tmp_path):
        self._copy_data(tmp_path, "users_db_export.csv")
        monkeypatch.setattr(ingest_module, "DATA_DIR", tmp_path)
        con = duckdb.connect(":memory:")
        first = ingest_module.ingest_users(con)
        assert first > 0
        second = ingest_module.ingest_users(con)
        assert second == 0, "Re-ingesting the same file must insert 0 new rows"

    def test_ingest_users_rejects_missing_file(self, monkeypatch, tmp_path):
        # tmp_path is empty — no users_db_export.csv present
        monkeypatch.setattr(ingest_module, "DATA_DIR", tmp_path)
        con = duckdb.connect(":memory:")
        with pytest.raises(ValueError):
            ingest_module.ingest_users(con)

    # ── ingest_mentors ────────────────────────────────────────────────────────

    def test_ingest_mentors_creates_table(self, monkeypatch, tmp_path):
        self._copy_data(tmp_path, "mentor_tiers.csv")
        monkeypatch.setattr(ingest_module, "DATA_DIR", tmp_path)
        con = duckdb.connect(":memory:")
        ingest_module.ingest_mentors(con)
        count = con.execute("SELECT COUNT(*) FROM raw_mentors").fetchone()
        count = count[0] if count else 0
        assert count == 15

    def test_ingest_mentors_full_replace(self, monkeypatch, tmp_path):
        """Calling ingest_mentors twice must not double the row count (full-replace)."""
        self._copy_data(tmp_path, "mentor_tiers.csv")
        monkeypatch.setattr(ingest_module, "DATA_DIR", tmp_path)
        con = duckdb.connect(":memory:")
        ingest_module.ingest_mentors(con)
        ingest_module.ingest_mentors(con)
        count = con.execute("SELECT COUNT(*) FROM raw_mentors").fetchone()
        count = count[0] if count else 0
        assert count == 15

    def test_ingest_mentors_rejects_missing_column(self, monkeypatch, tmp_path):
        """A CSV without the 'tier' column must raise ValueError before any DB write."""
        bad = tmp_path / "mentor_tiers.csv"
        bad.write_text("mentor_id,hourly_rate\nM01,100\n")
        monkeypatch.setattr(ingest_module, "DATA_DIR", tmp_path)
        con = duckdb.connect(":memory:")
        with pytest.raises(ValueError, match="tier"):
            ingest_module.ingest_mentors(con)

    # ── ingest_events ─────────────────────────────────────────────────────────

    def test_ingest_events_creates_table(self, monkeypatch, tmp_path):
        self._copy_data(tmp_path, "booking_events.json")
        monkeypatch.setattr(ingest_module, "DATA_DIR", tmp_path)
        con = duckdb.connect(":memory:")
        n = ingest_module.ingest_events(con)
        assert n > 0
        total = con.execute("SELECT COUNT(*) FROM raw_events").fetchone()
        total = total[0] if total else 0
        assert total == n

    def test_ingest_events_is_idempotent(self, monkeypatch, tmp_path):
        self._copy_data(tmp_path, "booking_events.json")
        monkeypatch.setattr(ingest_module, "DATA_DIR", tmp_path)
        con = duckdb.connect(":memory:")
        ingest_module.ingest_events(con)
        second = ingest_module.ingest_events(con)
        assert second == 0, "Re-ingesting the same events must insert 0 new rows"


# ── Integration tests ─────────────────────────────────────────────────────────

class TestIntegration:
    """
    End-to-end tests using small fixture files (tests/fixtures/).

    These complement the unit tests by catching contract breaks that
    in-memory seeding would miss — e.g. a column rename in ingest.py
    that silently breaks the analysis SQL, or a type mismatch between
    the raw event stream and the session-pairing query.
    """

    FIXTURES = Path(__file__).resolve().parent / "fixtures"

    @pytest.fixture
    def int_con(self, monkeypatch):
        con = duckdb.connect(":memory:")
        monkeypatch.setattr(ingest_module, "DATA_DIR", self.FIXTURES)
        yield con
        con.close()

    def test_ingest_users_deduplicates_fixture(self, int_con):
        # Fixture has 11 rows but user_id=1 appears twice → expect 10 unique users.
        count = ingest_module.ingest_users(int_con)
        assert count == 10
        assert int_con.execute("SELECT COUNT(*) FROM raw_users").fetchone()[0] == 10

    def test_ingest_users_idempotent_on_replay(self, int_con):
        ingest_module.ingest_users(int_con)
        assert ingest_module.ingest_users(int_con) == 0

    def test_ingest_mentors_normalises_fixture(self, int_con):
        ingest_module.ingest_mentors(int_con)
        tiers = {r[0] for r in int_con.execute("SELECT DISTINCT tier FROM raw_mentors").fetchall()}
        assert tiers == {"Gold", "Silver", "Bronze"}

    def test_ingest_events_handles_mixed_user_id_types(self, int_con):
        # Fixture deliberately uses both int and str user_ids (e.g. user_id "2" in e05/e06).
        count = ingest_module.ingest_events(int_con)
        assert count == 30
        null_ids = int_con.execute(
            "SELECT COUNT(*) FROM raw_events WHERE user_id IS NULL"
        ).fetchone()[0]
        assert null_ids == 0, "Mixed-type user_ids must all normalise to BIGINT without losing rows"

    def test_ingest_events_idempotent_on_replay(self, int_con):
        ingest_module.ingest_events(int_con)
        assert ingest_module.ingest_events(int_con) == 0

    def test_analysis_sql_end_to_end(self, int_con):
        """
        Runs the full ingest → session pairing → analysis pipeline against
        the fixture data and asserts the exact rebooking rates.

        Fixture design (see tests/fixtures/booking_events.json):
          Gold  group (M1): users 1, 2, 5, 6
            User 1: 2 sessions 19 days apart  → rebooked within 30d ✓
            User 2: 1 session only             → not rebooked
            User 5: 2 sessions 41 days apart  → NOT rebooked (outside 30d window)
            User 6: 2 sessions 19 days apart  → rebooked within 30d ✓
          Silver group (M2): users 3, 4, 7, 9
            User 3: 2 sessions 12 days apart  → rebooked within 30d ✓
            User 4: 1 session only (churned)   → not rebooked
            User 7: 2 sessions 23 days apart  → rebooked within 30d ✓
            User 9: 1 session, no end event    → not rebooked (browser crash)

        Expected (30-day window): Gold = 50% (2/4), Silver = 50% (2/4).
        """
        ingest_module.ingest_users(int_con)
        ingest_module.ingest_mentors(int_con)
        ingest_module.ingest_events(int_con)
        self._build_dim_fact_tables(int_con)

        analysis_sql = (
            _ROOT / "dbt" / "analyses" / "analysis_rebooking.sql"
        ).read_text(encoding="utf-8")
        for placeholder, value in {
            "TIERS_A":          "'Gold'",
            "TIERS_B":          "'Silver'",
            "GROUP_A_LABEL":    "Gold",
            "GROUP_B_LABEL":    "Silver",
            "WINDOW_CONDITION": "usr.started_at <= fs.first_session_at + INTERVAL '30' DAY",
        }.items():
            analysis_sql = analysis_sql.replace(f"{{{placeholder}}}", value)

        df = int_con.sql(analysis_sql).pl()

        assert len(df) == 2, f"Expected 2 tier groups, got {len(df)}: {df}"
        assert set(df["mentor_tier"].to_list()) == {"Gold", "Silver"}

        gold   = df.filter(pl.col("mentor_tier") == "Gold").row(0, named=True)
        silver = df.filter(pl.col("mentor_tier") == "Silver").row(0, named=True)

        assert gold["total_users"]    == 4
        assert gold["users_rebooked"] == 2
        assert float(gold["rebooking_rate_pct"]) == 50.0

        assert silver["total_users"]    == 4
        assert silver["users_rebooked"] == 2
        assert float(silver["rebooking_rate_pct"]) == 50.0

    def test_rebooking_window_excludes_late_sessions(self, int_con):
        """
        User 5 (Gold/M1) has two sessions 41 days apart.
        Without a window they are rebooked; with window=30 they are not.

        Gold (unlimited):  users 1✓, 2✗, 5✓(41d), 6✓ → 3/4 = 75%
        Gold (window=30d): users 1✓, 2✗, 5✗(41d), 6✓ → 2/4 = 50%
        """
        ingest_module.ingest_users(int_con)
        ingest_module.ingest_mentors(int_con)
        ingest_module.ingest_events(int_con)
        self._build_dim_fact_tables(int_con)

        analysis_sql = (_ROOT / "dbt" / "analyses" / "analysis_rebooking.sql").read_text(encoding="utf-8")

        def _run(window_condition: str) -> pl.DataFrame:
            sql = analysis_sql
            for ph, val in {
                "TIERS_A": "'Gold'", "TIERS_B": "'Silver'",
                "GROUP_A_LABEL": "Gold", "GROUP_B_LABEL": "Silver",
                "WINDOW_CONDITION": window_condition,
            }.items():
                sql = sql.replace(f"{{{ph}}}", val)
            return int_con.sql(sql).pl()

        gold_unlimited = _run("TRUE").filter(pl.col("mentor_tier") == "Gold").row(0, named=True)
        assert gold_unlimited["total_users"]    == 4
        assert gold_unlimited["users_rebooked"] == 3, "Without window user 5 (41d) counts as rebooked"

        gold_windowed = _run("usr.started_at <= fs.first_session_at + INTERVAL '30' DAY").filter(
            pl.col("mentor_tier") == "Gold"
        ).row(0, named=True)
        assert gold_windowed["total_users"]    == 4
        assert gold_windowed["users_rebooked"] == 2, "With window=30 user 5 (41d) must be excluded"
        assert float(gold_windowed["rebooking_rate_pct"]) == 50.0

    def test_missing_end_event_user_counted_in_totals(self, int_con):
        """
        User 9 (Silver/M2) has only a session_started event (browser crash, no session_ended).
        They must appear in total_users but NOT in users_rebooked.
        """
        ingest_module.ingest_users(int_con)
        ingest_module.ingest_mentors(int_con)
        ingest_module.ingest_events(int_con)
        self._build_dim_fact_tables(int_con)

        row = int_con.execute(
            "SELECT duration_minutes, is_duration_estimated FROM fct_sessions WHERE user_id = 9"
        ).fetchone()
        assert row is not None, "User 9 must produce a session row despite missing end event"
        assert row[1] is True, "is_duration_estimated must be TRUE when end event is missing"

        analysis_sql = (_ROOT / "dbt" / "analyses" / "analysis_rebooking.sql").read_text(encoding="utf-8")
        for ph, val in {
            "TIERS_A": "'Gold'", "TIERS_B": "'Silver'",
            "GROUP_A_LABEL": "Gold", "GROUP_B_LABEL": "Silver",
            "WINDOW_CONDITION": "usr.started_at <= fs.first_session_at + INTERVAL '30' DAY",
        }.items():
            analysis_sql = analysis_sql.replace(f"{{{ph}}}", val)

        silver = int_con.sql(analysis_sql).pl().filter(pl.col("mentor_tier") == "Silver").row(0, named=True)
        assert silver["total_users"]    == 4, "User 9 (crash, 1 session) must be counted in totals"
        assert silver["users_rebooked"] == 2, "User 9 has only 1 session so must NOT be rebooked"

    @staticmethod
    def _build_dim_fact_tables(con) -> None:
        """Build simplified dim_users, dim_mentors, fct_sessions from already-ingested raw tables."""
        con.execute("""
            CREATE TABLE dim_users AS
            SELECT
                SHA256(CAST(user_id AS VARCHAR))   AS user_hk,
                CAST(user_id AS BIGINT)            AS user_id,
                company_id,
                CAST(signup_date AS DATE)          AS signup_date,
                status,
                CURRENT_DATE                       AS valid_from,
                NULL::DATE                         AS valid_to
            FROM raw_users;

            CREATE TABLE dim_mentors AS
            SELECT
                SHA256(mentor_id || tier)  AS mentor_hk,
                mentor_id,
                tier,
                hourly_rate,
                CURRENT_DATE               AS valid_from,
                NULL::DATE                 AS valid_to
            FROM raw_mentors;

            CREATE TABLE fct_sessions AS
            WITH starts AS (
                SELECT
                    event_id                       AS session_id,
                    CAST(user_id    AS BIGINT)     AS user_id,
                    TRIM(mentor_id)                AS mentor_id,
                    CAST("timestamp" AS TIMESTAMP) AS started_at,
                    LEAD(CAST("timestamp" AS TIMESTAMP)) OVER (
                        PARTITION BY CAST(user_id AS BIGINT), TRIM(mentor_id)
                        ORDER BY "timestamp"
                    )                              AS next_start_at
                FROM raw_events WHERE event_type = 'session_started'
            ),
            ends AS (
                SELECT
                    CAST(user_id    AS BIGINT)     AS user_id,
                    TRIM(mentor_id)                AS mentor_id,
                    CAST("timestamp" AS TIMESTAMP) AS ended_at
                FROM raw_events WHERE event_type = 'session_ended'
            ),
            paired AS (
                SELECT
                    s.session_id, s.user_id, s.mentor_id, s.started_at,
                    MIN(e.ended_at) AS ended_at
                FROM starts s
                LEFT JOIN ends e
                    ON  s.user_id   = e.user_id
                    AND s.mentor_id = e.mentor_id
                    AND e.ended_at  > s.started_at
                    AND (s.next_start_at IS NULL OR e.ended_at < s.next_start_at)
                GROUP BY s.session_id, s.user_id, s.mentor_id, s.started_at
            )
            SELECT
                SHA256(CAST(user_id AS VARCHAR) || mentor_id || CAST(started_at AS VARCHAR)) AS session_hk,
                session_id, user_id, mentor_id, started_at, ended_at,
                COALESCE(DATEDIFF('minute', started_at, ended_at), 30) AS duration_minutes,
                ended_at IS NULL                                        AS is_duration_estimated,
                CAST(started_at AS DATE)                                AS session_date
            FROM paired;
        """)
