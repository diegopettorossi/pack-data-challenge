{{
    config(
        materialized = 'table'
    )
}}

-- Mentor dimension — reads the snap_mentors snapshot which manages SCD2 lifecycle.
-- dbt snapshot fields: dbt_valid_from, dbt_valid_to, dbt_scd_id.
-- This model adds a stable SHA-256 surrogate key (date-based, not timestamp)
-- and aligns column names with the rest of the mart.
--
-- Materialised as 'table' (not incremental) because the mentor list is small
-- and incremental merge on a timestamp-derived SHA-256 key accumulated
-- duplicate rows whenever dbt run was called more than once in a session.
--
-- QUALIFY ensures one row per (mentor_id, valid_from) even if the snapshot
-- somehow emits multiple entries for the same SCD2 change date.
--
-- is_current = (valid_to IS NULL) — evaluate at query time.

SELECT
    SHA256(
        mentor_id || '|' || tier || '|' || CAST(CAST(dbt_valid_from AS DATE) AS VARCHAR)
    )                                               AS mentor_hk,
    mentor_id,
    tier,
    hourly_rate,
    CAST(dbt_valid_from AS DATE)                    AS valid_from,
    CAST(dbt_valid_to   AS DATE)                    AS valid_to,
    dbt_valid_from                                  AS inserted_at
FROM {{ ref('snap_mentors') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY mentor_id, CAST(dbt_valid_from AS DATE), tier
    ORDER BY dbt_valid_from DESC
) = 1
