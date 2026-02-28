{{ config(materialized='view') }}

-- De-duplicate and cast raw users.
-- ROW_NUMBER keeps the earliest-signup row per user_id.

WITH deduped AS (
    SELECT
        CAST(user_id    AS BIGINT)  AS user_id,
        CAST(company_id AS BIGINT)  AS company_id,
        CAST(signup_date AS DATE)   AS signup_date,
        status,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(user_id AS BIGINT)
            ORDER BY signup_date ASC
        ) AS rn
    FROM {{ source('raw', 'raw_users') }}
)
SELECT user_id, company_id, signup_date, status
FROM deduped
WHERE rn = 1
