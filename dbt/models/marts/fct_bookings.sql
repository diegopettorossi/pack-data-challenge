{{
    config(
        materialized        = 'incremental',
        unique_key          = 'booking_hk',
        incremental_strategy = 'merge'
    )
}}

-- Booking fact table — incremental, deduplicated by SHA-256 surrogate key.
--
-- booking_hk  = SHA256(user_id | mentor_id | requested_at) — DB-layer dedup key.
-- booking_id  = source event_id of the booking_requested event (business key).
-- inserted_at = warehouse load timestamp.
--
-- outcome_status derives the full booking lifecycle state:
--   'confirmed_attended' : booking_confirmed AND a session_started followed
--   'no_show'            : booking_confirmed AND no session_started detected
--   'cancelled'          : booking_cancelled after booking_requested
--   'pending'            : booking_requested with no follow-up (DQ orphan)
--
-- Pairing and no-show detection logic lives in stg_booking_pairs.
-- The LEAD-bounded join there ensures each outcome event is assigned to
-- exactly one booking_requested row, preventing double counting.

SELECT
    SHA256(
        CAST(user_id AS VARCHAR) || '|' ||
        mentor_id || '|' ||
        CAST(requested_at AS VARCHAR)
    )                                                   AS booking_hk,
    booking_request_event_id                            AS booking_id,
    user_id,
    mentor_id,
    requested_at,
    outcome_at,
    first_session_at,
    CASE
        WHEN outcome_type = 'booking_confirmed' AND had_session_started THEN 'confirmed_attended'
        WHEN outcome_type = 'booking_confirmed' AND NOT had_session_started THEN 'no_show'
        WHEN outcome_type = 'booking_cancelled'                            THEN 'cancelled'
        ELSE                                                                    'pending'
    END                                                 AS outcome_status,
    is_orphan_request,
    CURRENT_TIMESTAMP                                   AS inserted_at
FROM {{ ref('stg_booking_pairs') }}

{% if is_incremental() %}
WHERE SHA256(
          CAST(user_id AS VARCHAR) || '|' ||
          mentor_id || '|' ||
          CAST(requested_at AS VARCHAR)
      ) NOT IN (SELECT booking_hk FROM {{ this }})
{% endif %}
