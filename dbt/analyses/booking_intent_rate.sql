-- Booking intent-to-rebook analysis by mentor tier (Gold vs Silver/Bronze).
--
-- Answers the alternative CEO framing:
--   "What percentage of users demonstrated intent to rebook within 30 days
--    AND eventually showed up — regardless of when the session took place?"
--
-- The 30-day window gates only the *booking request* (intent signal).
-- Attendance (confirmed_attended outcome) can occur at any time after confirmation;
-- no time cap is applied to the session itself.
--
-- Unlike analysis_rebooking.sql (which counts any second session in fct_sessions),
-- this query restricts the numerator to users who explicitly scheduled through the
-- booking system (fct_bookings: booking_requested → confirmed → attended).
-- Intent is therefore observable and formal, not inferred from session occurrence.
--
-- Metrics produced per tier group:
--   total_users          — distinct users attributed to that tier via first session
--   users_with_intent    — users who placed a booking request within 30d of first session
--   users_intent_attended — intent users whose booking reached confirmed_attended
--   users_intent_noshow  — intent users whose confirmed booking was a no-show
--   users_intent_cancelled — intent users who cancelled before confirmation
--   intent_rate_pct      — users_with_intent / total_users × 100
--   intent_attended_pct  — users_intent_attended / total_users × 100
--   commitment_rate_pct  — users_intent_attended / users_with_intent × 100
--
-- Lives in analyses/ — dbt compiles it (resolves ref() calls) but does NOT
-- execute it. run_pipeline.py (or a direct DuckDB connection) executes it
-- against the materialised mart tables.
--
-- Table names match dbt-materialised names; plain SQL (no Jinja needed).

WITH user_first_session AS (
    -- One row per user: their first session timestamp + that mentor's current tier.
    SELECT
        s.user_id,
        s.started_at  AS first_session_at,
        m.tier        AS mentor_tier
    FROM fct_sessions s
    INNER JOIN dim_mentors m
        ON  s.mentor_id  = m.mentor_id
        AND m.valid_to  IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.user_id ORDER BY s.started_at) = 1
),

-- All booking requests placed within 30 days after the user's first session.
-- No time cap on the outcome: a booking confirmed after day 30 still counts
-- as long as the *request* was placed within the 30-day intent window.
rebooking_bookings AS (
    SELECT
        fb.user_id,
        fb.outcome_status,
        fb.requested_at
    FROM fct_bookings fb
    INNER JOIN user_first_session ufs
        ON  fb.user_id       = ufs.user_id
        AND fb.requested_at  > ufs.first_session_at
        AND fb.requested_at <= ufs.first_session_at + INTERVAL '30 days'
)

SELECT
    CASE
        WHEN ufs.mentor_tier = 'Gold' THEN 'Gold'
        ELSE 'Silver/Bronze'
    END                                                                                            AS tier_group,

    COUNT(DISTINCT ufs.user_id)                                                                    AS total_users,

    -- Intent: placed any booking request within 30d of first session
    COUNT(DISTINCT CASE WHEN rb.user_id IS NOT NULL
                        THEN ufs.user_id END)                                                      AS users_with_intent,

    -- Intent + attended: booking reached confirmed_attended state
    COUNT(DISTINCT CASE WHEN rb.outcome_status = 'confirmed_attended'
                        THEN ufs.user_id END)                                                      AS users_intent_attended,

    -- Intent but no-show: confirmed booking, user did not appear
    COUNT(DISTINCT CASE WHEN rb.outcome_status = 'no_show'
                        THEN ufs.user_id END)                                                      AS users_intent_noshow,

    -- Intent but cancelled: booking cancelled before or after confirmation
    COUNT(DISTINCT CASE WHEN rb.outcome_status = 'cancelled'
                        THEN ufs.user_id END)                                                      AS users_intent_cancelled,

    -- % of all users who expressed formal rebooking intent
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN rb.user_id IS NOT NULL        THEN ufs.user_id END)
              / NULLIF(COUNT(DISTINCT ufs.user_id), 0),
    1)                                                                                             AS intent_rate_pct,

    -- % of all users who intended AND attended (headline metric)
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN rb.outcome_status = 'confirmed_attended' THEN ufs.user_id END)
              / NULLIF(COUNT(DISTINCT ufs.user_id), 0),
    1)                                                                                             AS intent_attended_pct,

    -- % of intenders who followed through (commitment rate)
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN rb.outcome_status = 'confirmed_attended' THEN ufs.user_id END)
              / NULLIF(COUNT(DISTINCT CASE WHEN rb.user_id IS NOT NULL THEN ufs.user_id END), 0),
    1)                                                                                             AS commitment_rate_pct

FROM user_first_session ufs
LEFT JOIN rebooking_bookings rb ON ufs.user_id = rb.user_id
GROUP BY tier_group
ORDER BY tier_group
