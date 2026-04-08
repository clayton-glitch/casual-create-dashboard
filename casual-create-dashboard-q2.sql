/*
Prompt 1: Write a query that returns the percent of qualified (or eligible) WAU that perform each of the create actions (i.e. own_song_listening, new_clip_generation, etc.)
 */

SELECT
    ab.p_date
    , ab.action_category
    , ab.action_name
    , ab.wau_eligible_nonbot                                        AS n_eligible_nonbot_action
    , m.wau_eligible_nonbot                                         AS n_eligible_nonbot_total
    , DIV0(ab.wau_eligible_nonbot, m.wau_eligible_nonbot)           AS pct_of_eligible_wau
FROM suno_prod.prod_marts.ce_wau_action_breakdown ab
INNER JOIN suno_prod.prod_marts.ce_wau_metrics m
    ON ab.p_date = m.p_date
WHERE ab.p_date = (SELECT MAX(p_date) FROM suno_prod.prod_marts.ce_wau_action_breakdown WHERE p_date < CURRENT_DATE())
ORDER BY ab.p_date, ab.action_category, ab.action_name
;
/*
  CLAUDE QUERY
  user:      clayton
  question:  Percent of eligible WAU performing each CE action
  purpose:   Action-level penetration rates against eligible nonbot WAU denominator
  attempt:   1
  timestamp: 20260407_220000
*/

/*
Prompt 2: Distribution of eligible WAU by CE days and Create days. Focus on most recent 7-day period.
Return table in narrow format (i.e. only one column for day counts, and one column indicating whether the count is for CE days or Create days)
 */

WITH latest_date AS (
    SELECT MAX(p_date) AS p_date
    FROM suno_prod.prod_int.int_habitual_creator_daily
    WHERE p_date < CURRENT_DATE()
)

, eligible_wau AS (
    SELECT au.user_id
    FROM latest_date ld
    INNER JOIN suno_prod.prod_marts.active_users_daily_v2 au
        ON au.p_date BETWEEN DATEADD(DAY, -6, ld.p_date::DATE) AND ld.p_date::DATE
        AND au.is_active = TRUE
    INNER JOIN suno_prod.prod.dim_user du
        ON au.user_id = du.user_id
        AND DATE(du.user_joined_at) <= DATEADD(DAY, -6, ld.p_date::DATE)
    LEFT JOIN suno_prod.prod.bot_hourly b
        ON au.user_id = b.user_id
    WHERE COALESCE(b.meets_bot_definition, FALSE) = FALSE
    GROUP BY au.user_id
)

, user_days AS (
    SELECT
        ew.user_id
        , COALESCE(h.ce_days_last_7, 0)     AS ce_days_last_7
        , COALESCE(h.create_days_last_7, 0) AS create_days_last_7
    FROM eligible_wau ew
    CROSS JOIN latest_date ld
    LEFT JOIN suno_prod.prod_int.int_habitual_creator_daily h
        ON ew.user_id = h.user_id
        AND h.p_date = ld.p_date
)

SELECT ld.p_date, 'Creative Entertainment Days'     AS metric, ud.ce_days_last_7     AS n_days, COUNT(*) AS n_users
FROM user_days ud CROSS JOIN latest_date ld
GROUP BY ld.p_date, metric, n_days

UNION ALL

SELECT ld.p_date, 'Creative Days' AS metric, ud.create_days_last_7 AS n_days, COUNT(*) AS n_users
FROM user_days ud CROSS JOIN latest_date ld
GROUP BY ld.p_date, metric, n_days

ORDER BY metric, n_days
;
/*
  CLAUDE QUERY
  user:      clayton
  question:  Distribution of eligible WAU by CE days and Create days
  purpose:   Narrow-format distribution (metric column + day count) for most recent window
  attempt:   2
  timestamp: 20260407_221500
*/


/*
Prompt 3: No we want: % of eligible WAU that have at N or more active days per week for the most recent 7-day period. 
Count number of Creative Entertainment Days and Creative Days separately. 
Return results in long format (i.e. only one column for day counts, and one column indicating whether the count is for CE days or Create days)
 */

WITH latest_date AS (
    SELECT MAX(p_date) AS p_date
    FROM suno_prod.prod_int.int_habitual_creator_daily
    WHERE p_date < CURRENT_DATE()
)

, eligible_wau AS (
    SELECT au.user_id
    FROM latest_date ld
    INNER JOIN suno_prod.prod_marts.active_users_daily_v2 au
        ON au.p_date BETWEEN DATEADD(DAY, -6, ld.p_date::DATE) AND ld.p_date::DATE
        AND au.is_active = TRUE
    INNER JOIN suno_prod.prod.dim_user du
        ON au.user_id = du.user_id
        AND DATE(du.user_joined_at) <= DATEADD(DAY, -6, ld.p_date::DATE)
    LEFT JOIN suno_prod.prod.bot_hourly b
        ON au.user_id = b.user_id
    WHERE COALESCE(b.meets_bot_definition, FALSE) = FALSE
    GROUP BY au.user_id
)

, user_days AS (
    SELECT
        ew.user_id
        , COALESCE(h.ce_days_last_7, 0)     AS ce_days
        , COALESCE(h.create_days_last_7, 0) AS create_days
    FROM eligible_wau ew
    CROSS JOIN latest_date ld
    LEFT JOIN suno_prod.prod_int.int_habitual_creator_daily h
        ON ew.user_id = h.user_id
        AND h.p_date = ld.p_date
)

, thresholds AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS min_days
    FROM TABLE(GENERATOR(ROWCOUNT => 8))
)

, totals AS (
    SELECT COUNT(*) AS n_eligible_wau FROM user_days
)

SELECT
    ld.p_date
    , v.metric
    , v.min_days
    , v.n_users
    , t.n_eligible_wau
    , DIV0(v.n_users, t.n_eligible_wau) AS pct_of_eligible_wau
FROM (
    SELECT 'ce_days' AS metric, th.min_days, COUNT_IF(ud.ce_days >= th.min_days) AS n_users
    FROM user_days ud CROSS JOIN thresholds th
    GROUP BY th.min_days

    UNION ALL

    SELECT 'create_days' AS metric, th.min_days, COUNT_IF(ud.create_days >= th.min_days) AS n_users
    FROM user_days ud CROSS JOIN thresholds th
    GROUP BY th.min_days
) v
CROSS JOIN latest_date ld
CROSS JOIN totals t
ORDER BY v.metric, v.min_days
;
/*
  CLAUDE QUERY
  user:      clayton
  question:  Pct of eligible WAU with N+ CE days and N+ Create days
  purpose:   Cumulative distribution (N=0..7) in long format for most recent window
  attempt:   1
  timestamp: 20260407_222000
*/

/*
Prompt 4: Find % of eligible WAU that were active in any capacity for three or more days in the most recent 7-day period.
 */

WITH latest_date AS (
    SELECT MAX(p_date) AS p_date
    FROM suno_prod.prod_marts.ce_wau_metrics
    WHERE p_date < CURRENT_DATE()
)

, eligible_wau AS (
    SELECT au.user_id, COUNT(DISTINCT au.p_date) AS active_days_last_7
    FROM latest_date ld
    INNER JOIN suno_prod.prod_marts.active_users_daily_v2 au
        ON au.p_date BETWEEN DATEADD(DAY, -6, ld.p_date::DATE) AND ld.p_date::DATE
        AND au.is_active = TRUE
    INNER JOIN suno_prod.prod.dim_user du
        ON au.user_id = du.user_id
        AND DATE(du.user_joined_at) <= DATEADD(DAY, -6, ld.p_date::DATE)
    LEFT JOIN suno_prod.prod.bot_hourly b
        ON au.user_id = b.user_id
    WHERE COALESCE(b.meets_bot_definition, FALSE) = FALSE
    GROUP BY au.user_id
)

SELECT
    ld.p_date
    , COUNT(*)                                AS n_eligible_wau
    , COUNT_IF(active_days_last_7 >= 3)       AS n_active_3plus_days
    , DIV0(COUNT_IF(active_days_last_7 >= 3), COUNT(*)) AS pct_active_3plus_days
FROM eligible_wau ew
CROSS JOIN latest_date ld
GROUP BY ld.p_date
;
/*
  CLAUDE QUERY
  user:      clayton
  question:  Pct of eligible WAU active (any capacity) 3+ days in most recent window
  purpose:   Single-row summary: eligible WAU with 3+ active days out of 7
  attempt:   1
  timestamp: 20260407_222500
*/

/* 
Prompt 5: Find the % of DAU with at least 1 create action (not creative entertainment action) on their active day. Average over the last 7 days.
 */

WITH latest_date AS (
    SELECT MAX(p_date) AS p_date
    FROM suno_prod.prod_int.int_ce_actions_user_daily
    WHERE p_date < CURRENT_DATE()
)

, daily_stats AS (
    SELECT
        au.p_date
        , COUNT(DISTINCT au.user_id)                                              AS n_dau
        , COUNT(DISTINCT CASE WHEN ce.has_create_action THEN au.user_id END)      AS n_dau_with_create
        , DIV0(
            COUNT(DISTINCT CASE WHEN ce.has_create_action THEN au.user_id END),
            COUNT(DISTINCT au.user_id)
          )                                                                       AS pct_dau_with_create
    FROM latest_date ld
    INNER JOIN suno_prod.prod_marts.active_users_daily_v2 au
        ON au.p_date BETWEEN DATEADD(DAY, -6, ld.p_date::DATE) AND ld.p_date::DATE
        AND au.is_active = TRUE
    LEFT JOIN suno_prod.prod.bot_hourly b
        ON au.user_id = b.user_id
    LEFT JOIN suno_prod.prod_int.int_ce_actions_user_daily ce
        ON au.user_id = ce.user_id
        AND au.p_date = ce.p_date
    WHERE COALESCE(b.meets_bot_definition, FALSE) = FALSE
    GROUP BY au.p_date
)

SELECT
    MIN(p_date)                    AS period_start
    , MAX(p_date)                  AS period_end
    , AVG(pct_dau_with_create)     AS avg_pct_dau_with_create
    , SUM(n_dau_with_create)       AS total_creators_7d
    , SUM(n_dau)                   AS total_dau_7d
FROM daily_stats
;
/*
  CLAUDE QUERY
  user:      clayton
  question:  Pct of DAU with at least 1 create action, averaged over last 7 days
  purpose:   Daily create-action penetration of nonbot DAU, 7-day average
  attempt:   1
  timestamp: 20260407_223000
*/

/*
Prompt 6: Find the habitual creator rate among eligible WAU over time.
 */

SELECT
    p_date
    , eligible_wau_nonbot
    , wau_habitual_creators_nonbot
    , habitual_creator_pct_of_wau_nonbot
FROM suno_prod.prod_marts.habitual_creator_metrics
WHERE p_date >= '2025-06-14'
  AND p_date < CURRENT_DATE()
ORDER BY p_date
;
/*
  CLAUDE QUERY
  user:      clayton
  question:  Habitual creator rate among eligible WAU over time
  purpose:   Time series of habitual creator pct from pre-aggregated mart
  attempt:   1
  timestamp: 20260407_223500
*/

/*
Prompt 7: Find the week-over-week retention rate of habitual creators over the same time period as Prompt 6. 
The habitual retention rate on Day N should be the percent of users who were habitual on Day N-7 who are also habitual on Day N. 
 */

WITH current_day AS (
    SELECT p_date, user_id
    FROM suno_prod.prod_int.int_habitual_creator_daily
    WHERE is_habitual_creator = TRUE
      AND p_date >= '2025-06-14'
      AND p_date < CURRENT_DATE()
)

, prior_week AS (
    SELECT p_date, user_id
    FROM suno_prod.prod_int.int_habitual_creator_daily
    WHERE is_habitual_creator = TRUE
      AND p_date >= DATEADD(DAY, -7, '2025-06-14'::DATE)
      AND p_date < CURRENT_DATE()
)

, bot AS (
    SELECT user_id
    FROM suno_prod.prod.bot_hourly
    WHERE meets_bot_definition = TRUE
)

SELECT
    DATEADD(DAY, 7, pw.p_date)::DATE                          AS p_date
    , COUNT(DISTINCT pw.user_id)                               AS n_habitual_prior_week
    , COUNT(DISTINCT c.user_id)                                AS n_retained
    , DIV0(COUNT(DISTINCT c.user_id), COUNT(DISTINCT pw.user_id)) AS habitual_wow_retention
FROM prior_week pw
LEFT JOIN current_day c
    ON pw.user_id = c.user_id
    AND c.p_date = DATEADD(DAY, 7, pw.p_date)
LEFT JOIN bot b ON pw.user_id = b.user_id
WHERE b.user_id IS NULL
  AND DATEADD(DAY, 7, pw.p_date) < CURRENT_DATE()
GROUP BY pw.p_date
ORDER BY p_date
;
/*
  CLAUDE QUERY
  user:      clayton
  question:  WoW retention of habitual creators (D-7 cohort comparison)
  purpose:   Pct of users habitual on D-7 who are still habitual on D, nonbot
  attempt:   2
  timestamp: 20260407_225000
*/


/*
Prompt 8: Find the % of habitual creators who perform each action category over the most recent 7-day period. 
 */

WITH latest_date AS (
    SELECT MAX(p_date) AS p_date
    FROM suno_prod.prod_int.int_habitual_creator_daily
    WHERE p_date < CURRENT_DATE()
)

, habitual_users AS (
    SELECT h.user_id
    FROM latest_date ld
    INNER JOIN suno_prod.prod_int.int_habitual_creator_daily h
        ON h.p_date = ld.p_date
        AND h.is_habitual_creator = TRUE
    LEFT JOIN suno_prod.prod.bot_hourly b ON h.user_id = b.user_id
    WHERE COALESCE(b.meets_bot_definition, FALSE) = FALSE
)

, user_actions AS (
    SELECT
        hu.user_id
        , MAX(CASE WHEN ce.has_new_content_generation THEN 1 ELSE 0 END) = 1 AS had_new_content_generation
        , MAX(CASE WHEN ce.has_remix THEN 1 ELSE 0 END) = 1                  AS had_content_editing_remixing
        , MAX(CASE WHEN ce.has_creative_process THEN 1 ELSE 0 END) = 1       AS had_creative_process
        , MAX(CASE WHEN ce.has_studio_session THEN 1 ELSE 0 END) = 1         AS had_studio_session
        , MAX(CASE WHEN ce.has_non_create_ce THEN 1 ELSE 0 END) = 1          AS had_non_create_ce_action
    FROM habitual_users hu
    CROSS JOIN latest_date ld
    LEFT JOIN suno_prod.prod_int.int_ce_actions_user_daily ce
        ON hu.user_id = ce.user_id
        AND ce.p_date BETWEEN DATEADD(DAY, -6, ld.p_date::DATE) AND ld.p_date::DATE
    GROUP BY hu.user_id
)

, totals AS (
    SELECT COUNT(*) AS n_habitual FROM user_actions
)

SELECT
    ld.p_date
    , v.action_category
    , v.n_users
    , t.n_habitual
    , DIV0(v.n_users, t.n_habitual) AS pct_of_habitual_creators
FROM (
    SELECT 'new_content_generation'   AS action_category, COUNT_IF(had_new_content_generation)  AS n_users FROM user_actions
    UNION ALL
    SELECT 'content_editing_remixing' AS action_category, COUNT_IF(had_content_editing_remixing) AS n_users FROM user_actions
    UNION ALL
    SELECT 'creative_process'         AS action_category, COUNT_IF(had_creative_process)         AS n_users FROM user_actions
    UNION ALL
    SELECT 'studio_session'           AS action_category, COUNT_IF(had_studio_session)           AS n_users FROM user_actions
    UNION ALL
    SELECT 'non_create_ce_action'     AS action_category, COUNT_IF(had_non_create_ce_action)     AS n_users FROM user_actions
) v
CROSS JOIN latest_date ld
CROSS JOIN totals t
ORDER BY pct_of_habitual_creators DESC
;
/*
  CLAUDE QUERY
  user:      clayton
  question:  Pct of habitual creators performing each action category
  purpose:   Action category penetration among habitual creators, most recent 7-day window
  attempt:   1
  timestamp: 20260407_225500
*/

/*
Prompt 9: For habitual users, construct the array of action_typed performed over the most recent 7-day period. Order alphabetically.
Return the percent of users by array. 
 */

WITH latest_date AS (
    SELECT MAX(p_date) AS p_date
    FROM suno_prod.prod_int.int_habitual_creator_daily
    WHERE p_date < CURRENT_DATE()
)

, habitual_users AS (
    SELECT h.user_id
    FROM latest_date ld
    INNER JOIN suno_prod.prod_int.int_habitual_creator_daily h
        ON h.p_date = ld.p_date
        AND h.is_habitual_creator = TRUE
    LEFT JOIN suno_prod.prod.bot_hourly b ON h.user_id = b.user_id
    WHERE COALESCE(b.meets_bot_definition, FALSE) = FALSE
)

, user_action_names AS (
    SELECT DISTINCT
        hu.user_id
        , f.key AS action_name
    FROM habitual_users hu
    CROSS JOIN latest_date ld
    INNER JOIN suno_prod.prod_int.int_ce_actions_user_daily ce
        ON hu.user_id = ce.user_id
        AND ce.p_date BETWEEN DATEADD(DAY, -6, ld.p_date::DATE) AND ld.p_date::DATE
    , LATERAL FLATTEN(input => ce.ce_action_detail) f
    WHERE f.value::NUMBER > 0
)

, user_arrays AS (
    SELECT
        user_id
        , ARRAY_AGG(action_name) WITHIN GROUP (ORDER BY action_name) AS action_types
    FROM user_action_names
    GROUP BY user_id
)

, totals AS (
    SELECT COUNT(*) AS n_habitual FROM habitual_users
)

SELECT
    ld.p_date
    , ua.action_types::VARCHAR AS action_types
    , COUNT(*)                 AS n_users
    , t.n_habitual             AS n_habitual_total
    , DIV0(COUNT(*), t.n_habitual) AS pct_of_habitual_creators
FROM user_arrays ua
CROSS JOIN latest_date ld
CROSS JOIN totals t
GROUP BY ld.p_date, ua.action_types::VARCHAR, t.n_habitual
ORDER BY n_users DESC
;
/*
  CLAUDE QUERY
  user:      clayton
  question:  Distribution of habitual creators by action-type portfolio
  purpose:   Sorted array of distinct action types per user, grouped by portfolio
  attempt:   1
  timestamp: 20260407_230000
*/


/*
Prompt 10: Find % of Pro/Premier/Free subscribers who perform a create action per 7-day period over time. 
Return table in long format (i.e. only one column for day counts, and one column indicating whether the count is for Pro/Premier/Free subscribers)
 */
WITH weeks AS (
    SELECT date_actual AS week_start
    FROM suno_prod.prod.dim_date
    WHERE date_actual >= '2025-06-16'
      AND date_actual < CURRENT_DATE()
      AND DAYOFWEEK(date_actual) = 1
)

, tier_users AS (
    SELECT w.week_start, ut.user_id, ut.subscription_tier
    FROM weeks w
    INNER JOIN suno_prod.prod_marts.user_subscription_tier_daily ut
        ON ut.p_date = w.week_start
        AND ut.subscription_tier IN ('Free Plan', 'Premier Plan', 'Pro Plan')
    LEFT JOIN suno_prod.prod.bot_hourly b ON ut.user_id = b.user_id
    WHERE COALESCE(b.meets_bot_definition, FALSE) = FALSE
)

, creators AS (
    SELECT w.week_start, ce.user_id
    FROM weeks w
    INNER JOIN suno_prod.prod_int.int_ce_actions_user_daily ce
        ON ce.p_date BETWEEN w.week_start AND DATEADD(DAY, 6, w.week_start)
        AND ce.has_create_action = TRUE
    WHERE ce.p_date >= '2025-06-16'
      AND ce.p_date < CURRENT_DATE()
    GROUP BY w.week_start, ce.user_id
)

SELECT
    tu.week_start
    , tu.subscription_tier
    , COUNT(DISTINCT tu.user_id)  AS n_tier_users
    , COUNT(DISTINCT c.user_id)   AS n_with_create
    , DIV0(COUNT(DISTINCT c.user_id), COUNT(DISTINCT tu.user_id)) AS pct_with_create_action
FROM tier_users tu
LEFT JOIN creators c
    ON tu.user_id = c.user_id
    AND tu.week_start = c.week_start
GROUP BY tu.week_start, tu.subscription_tier
ORDER BY tu.week_start, tu.subscription_tier
;
/*
  CLAUDE QUERY
  user:      clayton
  question:  Pct of all users by tier who perform a create action per calendar week
  purpose:   Create-action penetration by subscription tier (all tier users as denominator)
  attempt:   3
  timestamp: 20260407_231500
*/