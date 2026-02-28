{{ config(materialized='view') }}

-- Clean and normalise raw mentors.
-- mentor_id is TRIM()-ed here and again in downstream models for safety.

SELECT
    TRIM(mentor_id)              AS mentor_id,
    tier,
    CAST(hourly_rate AS INTEGER) AS hourly_rate
FROM {{ source('raw', 'raw_mentors') }}
