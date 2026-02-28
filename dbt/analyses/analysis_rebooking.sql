-- Rebooking-rate analysis by mentor tier.
--
-- Plain SQL template — run_pipeline.py and tests perform {PLACEHOLDER} substitution
-- at runtime so dbt is not required for this step.
--
-- Placeholders:
--   {TIERS_A}          — SQL IN-list for Group A  e.g. 'Gold', 'Platinum'
--   {TIERS_B}          — SQL IN-list for Group B  e.g. 'Silver', 'Bronze'
--   {GROUP_A_LABEL}    — display label for Group A  e.g. Gold/Platinum
--   {GROUP_B_LABEL}    — display label for Group B  e.g. Silver/Bronze
--   {WINDOW_CONDITION} — boolean SQL expression used in the CASE WHEN block:
--                          With window  : usr.started_at <= fs.first_session_at + INTERVAL 'N' DAY
--                          Unlimited    : TRUE
--
-- Table names match the dbt-materialised names so no ref() resolution is needed.
--
-- Output columns: mentor_tier, total_users, users_rebooked, rebooking_rate_pct

WITH

-- Deduplicate dim_mentors: one current row per mentor_id.
-- Defensive guard — the 'table' materialization prevents accumulation, but this
-- QUALIFY ensures correctness even if the warehouse state is stale.
current_mentors AS (
    SELECT mentor_id, tier
    FROM dim_mentors
    WHERE valid_to IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY mentor_id ORDER BY valid_from DESC) = 1
),

user_sessions_ranked AS (
    SELECT
        f.user_id,
        f.started_at,
        CASE
            WHEN m.tier IN ({TIERS_A}) THEN '{GROUP_A_LABEL}'
            WHEN m.tier IN ({TIERS_B}) THEN '{GROUP_B_LABEL}'
        END                                           AS tier_group,
        ROW_NUMBER() OVER (
            PARTITION BY f.user_id ORDER BY f.started_at
        )                                             AS session_rank
    FROM fct_sessions f
    INNER JOIN current_mentors m ON f.mentor_id = m.mentor_id
    WHERE m.tier IN ({TIERS_A}, {TIERS_B})
),

first_sessions AS (
    SELECT
        user_id,
        started_at  AS first_session_at,
        tier_group  AS first_tier_group
    FROM user_sessions_ranked
    WHERE session_rank = 1
),

rebooking_check AS (
    SELECT
        fs.user_id,
        fs.first_tier_group,
        MAX(
            CASE
                WHEN usr.session_rank >= 2
                 AND {WINDOW_CONDITION}
                THEN 1
                ELSE 0
            END
        ) AS rebooked_within_window
    FROM first_sessions fs
    LEFT JOIN user_sessions_ranked usr ON fs.user_id = usr.user_id
    GROUP BY fs.user_id, fs.first_tier_group
)

SELECT
    first_tier_group                            AS mentor_tier,
    COUNT(*)                                    AS total_users,
    SUM(rebooked_within_window)                 AS users_rebooked,
    ROUND(AVG(rebooked_within_window) * 100, 1) AS rebooking_rate_pct
FROM rebooking_check
GROUP BY first_tier_group
ORDER BY rebooking_rate_pct DESC
