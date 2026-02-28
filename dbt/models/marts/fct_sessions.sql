{{
    config(
        materialized       = 'incremental',
        unique_key         = 'session_hk',
        incremental_strategy = 'merge'
    )
}}

-- Session fact table — incremental, deduplicated by SHA-256 surrogate key.
--
-- session_id  = source event_id (business key; preserved for lineage).
-- session_hk  = SHA256(user_id | mentor_id | started_at) — DB-layer dedup key.
-- inserted_at = warehouse load timestamp.
--
-- Default duration for missing session_ended events is controlled by the dbt
-- variable default_duration_minutes (set in dbt_project.yml, overridable at run
-- time with: dbt run --vars '{"default_duration_minutes": 45}').

SELECT
    SHA256(
        CAST(user_id AS VARCHAR) || '|' ||
        mentor_id || '|' ||
        CAST(started_at AS VARCHAR)
    )                                                     AS session_hk,
    session_start_event_id                                AS session_id,
    user_id,
    mentor_id,
    started_at,
    COALESCE(
        ended_at,
        started_at + INTERVAL '{{ var("default_duration_minutes", 30) }}' MINUTE
    )                                                     AS ended_at,
    (ended_at IS NULL)                                    AS is_duration_estimated,
    DATE_DIFF(
        'minute',
        started_at,
        COALESCE(ended_at, started_at + INTERVAL '{{ var("default_duration_minutes", 30) }}' MINUTE)
    )                                                     AS duration_minutes,
    CAST(started_at AS DATE)                              AS session_date,
    CURRENT_TIMESTAMP                                     AS inserted_at
FROM {{ ref('stg_session_pairs') }}

{% if is_incremental() %}
WHERE SHA256(
          CAST(user_id AS VARCHAR) || '|' ||
          mentor_id || '|' ||
          CAST(started_at AS VARCHAR)
      ) NOT IN (SELECT session_hk FROM {{ this }})
{% endif %}
