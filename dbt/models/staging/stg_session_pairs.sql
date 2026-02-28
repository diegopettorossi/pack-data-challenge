{{ config(materialized='view') }}

-- Reconstruct paired sessions from the booking event stream.
--
-- Every session_started event becomes one row paired with the nearest valid
-- session_ended for the same (user_id, mentor_id).
--
-- LEAD() captures the next session_started timestamp for the same pair
-- and uses it as an upper bound for end-event matching, ensuring a missing
-- session_ended is isolated to exactly one session and does not cascade.

WITH session_starts AS (
    SELECT
        event_id                             AS session_start_event_id,
        CAST(user_id AS BIGINT)              AS user_id,
        TRIM(mentor_id)                      AS mentor_id,
        CAST("timestamp" AS TIMESTAMP)       AS started_at,
        LEAD(CAST("timestamp" AS TIMESTAMP)) OVER (
            PARTITION BY CAST(user_id AS BIGINT), TRIM(mentor_id)
            ORDER BY "timestamp" ASC
        )                                    AS next_start_at
    FROM {{ source('raw', 'raw_events') }}
    WHERE event_type = 'session_started'
),

session_ends AS (
    SELECT
        CAST(user_id AS BIGINT)        AS user_id,
        TRIM(mentor_id)                AS mentor_id,
        CAST("timestamp" AS TIMESTAMP) AS ended_at
    FROM {{ source('raw', 'raw_events') }}
    WHERE event_type = 'session_ended'
)

SELECT
    s.session_start_event_id,
    s.user_id,
    s.mentor_id,
    s.started_at,
    MIN(e.ended_at) AS ended_at   -- MIN guards against multiple ends per start
FROM session_starts s
LEFT JOIN session_ends e
    ON  s.user_id   = e.user_id
    AND s.mentor_id = e.mentor_id
    AND e.ended_at  > s.started_at
    AND (s.next_start_at IS NULL OR e.ended_at < s.next_start_at)
GROUP BY
    s.session_start_event_id,
    s.user_id,
    s.mentor_id,
    s.started_at
