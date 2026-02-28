{% snapshot snap_mentors %}
{{
    config(
        target_schema  = 'main',
        strategy       = 'check',
        unique_key     = 'mentor_id',
        check_cols     = ['tier'],
        invalidate_hard_deletes = True
    )
}}

-- dbt snapshots manage SCD2 automatically:
--   - New mentor    → INSERT with dbt_valid_from = NOW(), dbt_valid_to = NULL
--   - Tier change   → UPDATE old row dbt_valid_to = NOW(), INSERT new row
--   - No change     → no write (idempotent)
--
-- Fields added by dbt: dbt_scd_id, dbt_valid_from, dbt_valid_to, dbt_updated_at
-- dim_mentors.sql reads this snapshot and adds the SHA-256 surrogate key.

SELECT
    TRIM(mentor_id)              AS mentor_id,
    tier,
    CAST(hourly_rate AS INTEGER) AS hourly_rate
FROM {{ source('raw', 'raw_mentors') }}

{% endsnapshot %}
