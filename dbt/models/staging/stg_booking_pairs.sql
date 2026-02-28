{{ config(materialized='view') }}

-- Reconstruct paired bookings from the event stream.
--
-- Each booking_requested becomes one row paired with its nearest subsequent
-- outcome event (booking_confirmed or booking_cancelled) for the same
-- (user_id, mentor_id) pair.
--
-- Double-counting prevention: LEAD() on booking_requested captures the next
-- request timestamp for the same pair.  This timestamp is used as an upper
-- bound when matching outcome events, so a single outcome is never claimed
-- by more than one booking_requested row.
--
-- No-show detection: for confirmed bookings we join session_started events
-- using the same LEAD upper bound.  If no session_started is found within
-- that window the booking is classified as a no-show in fct_bookings.
--
-- is_orphan_request = TRUE  →  booking_requested with no follow-up (DQ flag).

WITH booking_requests AS (
    SELECT
        event_id                             AS booking_request_event_id,
        CAST(user_id AS BIGINT)              AS user_id,
        TRIM(mentor_id)                      AS mentor_id,
        CAST("timestamp" AS TIMESTAMP)       AS requested_at,
        -- Upper bound: next booking_requested for the same pair
        LEAD(CAST("timestamp" AS TIMESTAMP)) OVER (
            PARTITION BY CAST(user_id AS BIGINT), TRIM(mentor_id)
            ORDER BY "timestamp" ASC
        )                                    AS next_request_at
    FROM {{ source('raw', 'raw_events') }}
    WHERE event_type = 'booking_requested'
),

booking_outcomes AS (
    SELECT
        CAST(user_id AS BIGINT)        AS user_id,
        TRIM(mentor_id)                AS mentor_id,
        event_type                     AS outcome_type,
        CAST("timestamp" AS TIMESTAMP) AS outcome_at
    FROM {{ source('raw', 'raw_events') }}
    WHERE event_type IN ('booking_confirmed', 'booking_cancelled')
),

session_starts AS (
    SELECT
        CAST(user_id AS BIGINT)        AS user_id,
        TRIM(mentor_id)                AS mentor_id,
        CAST("timestamp" AS TIMESTAMP) AS session_started_at
    FROM {{ source('raw', 'raw_events') }}
    WHERE event_type = 'session_started'
),

-- Match each booking_requested to the nearest outcome within the LEAD window.
-- MIN(outcome_at) ensures we take the first outcome if multiple exist (DQ edge case).
paired AS (
    SELECT
        br.booking_request_event_id,
        br.user_id,
        br.mentor_id,
        br.requested_at,
        br.next_request_at,
        MIN(bo.outcome_at)   AS outcome_at,
        MIN(bo.outcome_type) AS outcome_type   -- MIN is an arbitrary tie-break on true duplicates
    FROM booking_requests br
    LEFT JOIN booking_outcomes bo
        ON  br.user_id   = bo.user_id
        AND br.mentor_id = bo.mentor_id
        AND bo.outcome_at  > br.requested_at
        AND (br.next_request_at IS NULL OR bo.outcome_at < br.next_request_at)
    GROUP BY
        br.booking_request_event_id,
        br.user_id,
        br.mentor_id,
        br.requested_at,
        br.next_request_at
),

-- For confirmed bookings, attach the earliest session_started in the same
-- LEAD-bounded window to detect no-shows without double-counting sessions.
paired_with_session AS (
    SELECT
        p.booking_request_event_id,
        p.user_id,
        p.mentor_id,
        p.requested_at,
        p.next_request_at,
        p.outcome_at,
        p.outcome_type,
        MIN(s.session_started_at) AS first_session_at
    FROM paired p
    LEFT JOIN session_starts s
        ON  p.outcome_type      = 'booking_confirmed'
        AND s.user_id           = p.user_id
        AND s.mentor_id         = p.mentor_id
        AND s.session_started_at > p.outcome_at
        AND (p.next_request_at IS NULL OR s.session_started_at < p.next_request_at)
    GROUP BY
        p.booking_request_event_id,
        p.user_id,
        p.mentor_id,
        p.requested_at,
        p.next_request_at,
        p.outcome_at,
        p.outcome_type
)

SELECT
    booking_request_event_id,
    user_id,
    mentor_id,
    requested_at,
    outcome_at,
    outcome_type,
    first_session_at,
    (first_session_at IS NOT NULL) AS had_session_started,
    (outcome_type IS NULL)         AS is_orphan_request
FROM paired_with_session
