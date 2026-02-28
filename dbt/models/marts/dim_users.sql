{{
    config(
        materialized       = 'incremental',
        unique_key         = 'user_hk',
        incremental_strategy = 'merge'
    )
}}

-- User dimension — SCD2 with SHA-256 surrogate key.
-- valid_from = signup_date (natural lifecycle date; no attribute tracking needed for users).
-- valid_to   = NULL for all records (users have no tracked attribute changes in this dataset).
-- is_current = (valid_to IS NULL) — evaluate at query time.

SELECT
    SHA256(
        CAST(user_id AS VARCHAR) || '|' ||
        CAST(company_id AS VARCHAR) || '|' ||
        CAST(signup_date AS VARCHAR)
    )                    AS user_hk,
    user_id,
    company_id,
    signup_date,
    status,
    signup_date          AS valid_from,
    NULL::DATE           AS valid_to,
    CURRENT_TIMESTAMP    AS inserted_at
FROM {{ ref('stg_users') }}

{% if is_incremental() %}
-- On incremental runs, skip rows already present (idempotent).
WHERE SHA256(
          CAST(user_id AS VARCHAR) || '|' ||
          CAST(company_id AS VARCHAR) || '|' ||
          CAST(signup_date AS VARCHAR)
      ) NOT IN (SELECT user_hk FROM {{ this }})
{% endif %}
