-- Booking reliability analysis by mentor tier.
--
-- Reports all known tiers — no grouping parameters needed here.
-- (Grouping parameters are only relevant for analysis_rebooking.sql.)
--
-- Rates:
--   confirmation_rate_pct = confirmed / total_bookings × 100
--   no_show_rate_pct      = no_show  / confirmed × 100  (ghost-booking rate)
--   cancellation_rate_pct = cancelled / total_bookings × 100
--
-- pending_count flags DQ orphans (booking_requested with no follow-up event).
--
-- Lives in analyses/ so dbt compiles it (resolves ref() calls) but does NOT
-- execute it. run_pipeline.py reads the compiled SQL from
--   target/compiled/pack_data/analyses/booking_reliability.sql
-- and executes it directly against the open DuckDB connection.

-- Plain SQL — table names match dbt-materialised names; run_pipeline.py executes
-- this directly without dbt compile.

WITH bookings_with_tier AS (
    SELECT
        fb.booking_id,
        fb.outcome_status,
        m.tier AS mentor_tier
    FROM fct_bookings fb
    INNER JOIN dim_mentors m
        ON  fb.mentor_id = m.mentor_id
        AND m.valid_to IS NULL
)

SELECT
    mentor_tier,
    COUNT(*)                                                                       AS total_bookings,
    SUM(CASE WHEN outcome_status IN ('confirmed_attended', 'no_show') THEN 1 ELSE 0 END)
                                                                                   AS confirmed_count,
    SUM(CASE WHEN outcome_status = 'cancelled'                        THEN 1 ELSE 0 END)
                                                                                   AS cancelled_count,
    SUM(CASE WHEN outcome_status = 'no_show'                          THEN 1 ELSE 0 END)
                                                                                   AS no_show_count,
    SUM(CASE WHEN outcome_status = 'pending'                          THEN 1 ELSE 0 END)
                                                                                   AS pending_count,
    ROUND(
        100.0 * SUM(CASE WHEN outcome_status IN ('confirmed_attended', 'no_show') THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
    1)                                                                             AS confirmation_rate_pct,
    ROUND(
        100.0 * SUM(CASE WHEN outcome_status = 'no_show' THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN outcome_status IN ('confirmed_attended', 'no_show') THEN 1 ELSE 0 END), 0),
    1)                                                                             AS no_show_rate_pct,
    ROUND(
        100.0 * SUM(CASE WHEN outcome_status = 'cancelled' THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
    1)                                                                             AS cancellation_rate_pct
FROM bookings_with_tier
GROUP BY mentor_tier
ORDER BY total_bookings DESC
