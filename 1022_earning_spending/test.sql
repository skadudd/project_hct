--  CREATE or Replace TABLE ballosodeuk.ynam.rfm_table_shopby_survive_prop as (
WITH base_data AS (
  select *
  from
    (SELECT 
      b.wk_id as user_id,
      a.order_dt,
      b.gender,
      b.birth_year,
      row_number() OVER (PARTITION BY b.wk_id ORDER BY a.order_dt DESC) as recency_rank,
      row_number() OVER (PARTITION BY b.wk_id ORDER BY a.order_dt ASC) as purchase_rank
    FROM (
      SELECT member_no, order_dt
      FROM ballosodeuk.dw.fact_shopby_order
      GROUP BY member_no, order_dt
    ) a
    LEFT JOIN ballosodeuk.dw.dim_shopby_member b 
      ON a.member_no = b.member_no)
  where user_id is not null
),

recent_purchase as (
  SELECT 
    user_id,
    order_dt as latest_order_dt,
    date_diff(current_date(), order_dt, day) as days_since_last_purchase
  FROM base_data
  WHERE recency_rank = 1
),

purchase_intervals AS (
  SELECT 
    user_id,
    order_dt,
    purchase_rank,
    LEAD(order_dt) OVER (PARTITION BY user_id ORDER BY order_dt) as next_order_date,
    date_diff(
      LEAD(order_dt) OVER (PARTITION BY user_id ORDER BY order_dt),
      order_dt,
      day
    ) as days_between_orders
  FROM base_data
  WHERE purchase_rank <= 15
),

user_stats as (
  SELECT 
    user_id,
    stddev(days_between_orders) as cycle_stddev
  FROM purchase_intervals
  WHERE days_between_orders is not null
  GROUP BY user_id
)

,current_trailing AS (
  SELECT 
    user_id,
    round(avg(days_between_orders), 1) as current_trailing_term,
    count(*) as current_count
  FROM purchase_intervals
  WHERE days_between_orders IS NOT NULL
  AND purchase_rank >= 1 
  AND purchase_rank <= 3
  GROUP BY user_id
),

prev_trailing AS (
  SELECT 
    user_id,
    round(avg(days_between_orders), 1) as prev_trailing_term,
    count(*) as prev_count
  FROM purchase_intervals
  WHERE days_between_orders IS NOT NULL
  AND purchase_rank >= 2
  AND purchase_rank <= 4
  GROUP BY user_id
),

survival_base AS (
  SELECT 
    c.user_id,
    r.days_since_last_purchase,
    c.current_trailing_term,
    p.prev_trailing_term,
    s.cycle_stddev,
    ROUND(((c.current_trailing_term - p.prev_trailing_term) / 
      NULLIF(p.prev_trailing_term, 0)) * 100, 1) as cycle_change_rate,
    ROUND((s.cycle_stddev / NULLIF(c.current_trailing_term, 0)) * 100, 1) as cycle_variation_rate,
    b.gender,
    CAST(FLOOR((EXTRACT(YEAR FROM CURRENT_DATE()) - SAFE_CAST(b.birth_year AS INT64)) / 10) * 10 AS STRING) as age_group
  FROM current_trailing c
  LEFT JOIN prev_trailing p ON c.user_id = p.user_id
  LEFT JOIN user_stats s ON c.user_id = s.user_id
  LEFT JOIN recent_purchase r ON c.user_id = r.user_id
  LEFT JOIN base_data b ON c.user_id = b.user_id AND b.recency_rank = 1
),

term_stats AS (
  SELECT 
    current_trailing_term,
    -- 로그 변환 적용
    LN(NULLIF(current_trailing_term, 0)) as log_term,
    STDDEV(LN(NULLIF(current_trailing_term, 0))) OVER () as log_stddev,
    AVG(LN(NULLIF(current_trailing_term, 0))) OVER () as log_mean,
    STDDEV(current_trailing_term) OVER () as pop_stddev,
    AVG(current_trailing_term) OVER () as pop_mean
  FROM survival_base
  WHERE current_trailing_term IS NOT NULL
  AND current_trailing_term > 0  -- 0 이하 제외
),

-- term_stats에서 로그 변환 추가
median_stats AS ( 
  SELECT 
    APPROX_QUANTILES(current_trailing_term, 2)[OFFSET(1)] as median_term,
    -- 로그 변환된 중앙값 추가
    APPROX_QUANTILES(LN(NULLIF(current_trailing_term, 0)), 2)[OFFSET(1)] as log_median_term
  FROM survival_base
  WHERE current_trailing_term IS NOT NULL
  AND current_trailing_term > 0
),

mad_stats AS (
  SELECT
    -- 기존 MAD
    APPROX_QUANTILES(
      ABS(s.current_trailing_term - m.median_term), 
      2
    )[OFFSET(1)] as mad,
    -- 로그 변환된 MAD
    APPROX_QUANTILES(
      ABS(LN(NULLIF(s.current_trailing_term, 0)) - m.log_median_term),
      2
    )[OFFSET(1)] as log_mad
  FROM survival_base s
  CROSS JOIN median_stats m
  WHERE s.current_trailing_term IS NOT NULL
  AND s.current_trailing_term > 0
),

robust_bounds AS (
  SELECT
    s.user_id,
    s.current_trailing_term,
    t.pop_mean,
    -- 로그 변환된 modified z-score 계산
    0.6745 * (LN(NULLIF(s.current_trailing_term, 0)) - m.log_median_term) / NULLIF(mad.log_mad, 0) as modified_zscore,
    s.days_since_last_purchase
  FROM survival_base s
  CROSS JOIN (SELECT DISTINCT pop_mean FROM term_stats) t
  CROSS JOIN median_stats m
  CROSS JOIN mad_stats mad
  WHERE s.current_trailing_term IS NOT NULL
  AND s.current_trailing_term > 0
),

churn_data AS (
  SELECT 
    r.user_id,
    r.current_trailing_term,
    s.age_group,
    s.gender,
    CASE
      WHEN ABS(r.modified_zscore) > 3.5 THEN
        CASE WHEN r.days_since_last_purchase > r.pop_mean THEN 1 ELSE 0 END
      ELSE
        CASE WHEN r.days_since_last_purchase > r.current_trailing_term * 2 THEN 1 ELSE 0 END
    END as churn_flag
  FROM robust_bounds r
  LEFT JOIN survival_base s ON r.user_id = s.user_id
),

churn_group_count AS (
  SELECT 
    age_group,
    gender,
    COUNT(*) AS group_user_count
  FROM churn_data
  GROUP BY age_group, gender
),

average_churn_rate AS (
  SELECT 
    c.age_group,
    c.gender,
    SUM(c.churn_flag) / COUNT(*) AS avg_churn_rate,  -- 수정: 단순히 이탈한 사용자 비율 계산
    AVG(c.current_trailing_term) as avg_group_term
  FROM churn_data c
  GROUP BY c.age_group, c.gender
),

-- 생존분석
survival_base_aggregated AS (
  SELECT 
    time_point,
    s.age_group,
    s.gender,
    COUNT(*) AS n_risk,
    SUM(c.churn_flag) AS n_events
  FROM (
    SELECT 
      s.*,
      t as time_point
    FROM survival_base s
    CROSS JOIN UNNEST(generate_array(0, 364, 7)) as t
  ) s
  LEFT JOIN churn_data c ON s.user_id = c.user_id
  GROUP BY 
    time_point,
    age_group,
    gender
)

survival_base_aggregated AS (
  SELECT 
    t as time_point,
    s.age_group,
    s.gender,
    -- 해당 time_point까지 살아남은 사용자 수
    COUNT(DISTINCT CASE 
      WHEN s.days_since_last_purchase > t THEN s.user_id 
    END) AS n_risk,
    -- 해당 time_point 구간에서 이탈한 사용자 수
    COUNT(DISTINCT CASE 
      WHEN s.days_since_last_purchase BETWEEN t AND t + 7 
      AND c.churn_flag = 1 THEN s.user_id
    END) AS n_events
  FROM survival_base s
  CROSS JOIN UNNEST(generate_array(0, 364, 7)) as t
  LEFT JOIN churn_data c ON s.user_id = c.user_id
  GROUP BY 
    t,
    age_group,
    gender
)

,age_group_stats AS (
  SELECT 
    time_point,
    'ALL' as age_group,
    gender,
    SUM(n_risk) as n_risk,
    SUM(n_events) as n_events
  FROM survival_base_aggregated
  GROUP BY time_point, gender
),

combined_stats AS (
  SELECT 
    time_point,
    age_group,
    gender,
    GREATEST(n_risk, 1) as n_risk,  -- 0 방지
    n_events,
    -- 해당 연령대의 데이터 양
    SUM(GREATEST(n_risk, 1)) OVER (PARTITION BY age_group, gender) as group_data_points,
    -- 전체 평균 생존율 (모든 연령대)
    SAFE_DIVIDE(
      SUM(n_events) OVER (PARTITION BY CAST(time_point AS STRING), gender), 
      GREATEST(SUM(n_risk) OVER (PARTITION BY CAST(time_point AS STRING), gender), 1)
    ) as overall_rate
  FROM (
    SELECT * FROM survival_base_aggregated
    UNION ALL 
    SELECT * FROM age_group_stats
  )
),

smoothed_stats AS (
  SELECT 
    *,
    -- 데이터 양에 따른 가중치 (로그 스케일 적용, 0 방지)
    LEAST(LN(GREATEST(group_data_points, 1) + 1) / LN(1000), 1.0) as data_weight,
    -- 개별 생존율
    (1 - SAFE_DIVIDE(n_events, GREATEST(n_risk, 1))) as individual_rate,
    -- 평활화된 생존율
    CASE 
      WHEN age_group = 'ALL' THEN (1 - overall_rate)
      ELSE (
        LEAST(LN(GREATEST(group_data_points, 1) + 1) / LN(1000), 1.0) * (1 - SAFE_DIVIDE(n_events, GREATEST(n_risk, 1))) +
        (1 - LEAST(LN(GREATEST(group_data_points, 1) + 1) / LN(1000), 1.0)) * (1 - overall_rate)
      )
    END as smoothed_rate
  FROM combined_stats
)

,normalized_stats AS (
  SELECT 
    *,
    -- 지수 감소 적용 (180일 기준)
    smoothed_rate * EXP(-time_point / 180) as decay_rate,
    -- 이전까지의 최대 감소율 계산
    MIN(smoothed_rate * EXP(-time_point / 180)) OVER (
      PARTITION BY age_group, gender
      ORDER BY time_point
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as min_rate_so_far
  FROM smoothed_stats
),

km_estimate AS (
  SELECT 
    time_point,
    age_group,
    gender,
    n_risk,
    n_events,
    -- 단조 감소하는 생존율 보장
    GREATEST(decay_rate, min_rate_so_far) as survival_prob,
    -- 누적 생존확률
    EXP(
      SUM(LN(GREATEST(decay_rate, min_rate_so_far))) OVER (
        PARTITION BY age_group, gender 
        ORDER BY time_point
      )
    ) as cumulative_survival_prob
  FROM normalized_stats
  WHERE age_group != 'ALL'  -- 전체 통계는 제외
)

select * from km_estimate


-- ,individual_survival AS (
--   SELECT 
--     s.user_id,
--     s.days_since_last_purchase,
--     s.current_trailing_term,
--     c.churn_flag,
--     r.modified_zscore,
--     s.age_group,
--     s.gender,
--     acr.avg_churn_rate,
--     acr.avg_group_term,
--     -- 기본 생존확률과 코호트별 시점별 생존확률을 결합
--   CASE 
--     WHEN ABS(r.modified_zscore) > 3.5 THEN 
--       0.7 * EXP(-r.days_since_last_purchase / r.pop_mean) +
--       0.3 * COALESCE(
--         (SELECT cumulative_survival_prob 
--         FROM km_estimate k 
--         WHERE k.age_group = s.age_group 
--         AND k.gender = s.gender
--         AND k.time_point = FLOOR(s.days_since_last_purchase / 7) * 7),
--         1
--       )
--     ELSE
--       0.7 * EXP(-r.days_since_last_purchase / NULLIF(s.current_trailing_term, 0)) +
--       0.3 * COALESCE(
--         (SELECT cumulative_survival_prob 
--         FROM km_estimate k 
--         WHERE k.age_group = s.age_group 
--         AND k.gender = s.gender
--         AND k.time_point = FLOOR(s.days_since_last_purchase / 7) * 7),
--         1
--       )
--   END as survival_prob
--   FROM survival_base s
--   LEFT JOIN churn_data c ON s.user_id = c.user_id
--   LEFT JOIN robust_bounds r ON s.user_id = r.user_id
--   LEFT JOIN average_churn_rate acr 
--     ON s.age_group = acr.age_group AND s.gender = acr.gender
-- ),

-- final_analysis AS (
--   SELECT 
--     s.user_id,
--     s.days_since_last_purchase,
--     s.current_trailing_term,
--     s.prev_trailing_term,
--     round(s.cycle_stddev) as cycle_stddev,
--     i.modified_zscore,
--     s.age_group,
--     s.gender,
--     i.avg_churn_rate as demographic_churn_rate,
--     i.churn_flag,
--     round(i.survival_prob,2) as survival_prob,
--     CASE 
--     WHEN s.current_trailing_term IS NULL THEN 
--         -- 기본값도 경과 시간과 연동
--         GREATEST(30 - s.days_since_last_purchase, 1)
--     ELSE 
--         CASE 
--             WHEN ABS(i.modified_zscore) > 3.5 THEN 
--                 -- 이상치인 경우 전체 평균 사용
--                 GREATEST(
--                     ROUND(-r.pop_mean * LN(0.5) - s.days_since_last_purchase),
--                     1
--                 )
--             ELSE
--                 -- 정상인 경우 개인 구매주기 사용
--                 GREATEST(
--                     ROUND(-s.current_trailing_term * LN(0.5) - s.days_since_last_purchase),
--                     1
--                 )
--         END
--     END AS predicted_survival_time,
--     CASE 
--       WHEN i.survival_prob <= 0.2 THEN 'High-Risk'
--       WHEN i.survival_prob <= 0.5 THEN 'Medium-Risk'
--       WHEN i.survival_prob <= 0.8 THEN 'Low-Risk'
--       ELSE 'Safe'
--     END AS risk_level,
--     CASE 
--       WHEN s.current_trailing_term <= 7 THEN '초단기'
--       WHEN s.current_trailing_term <= 28 THEN '단기'
--       WHEN s.current_trailing_term <= 60 THEN '중기'
--       ELSE '장기'
--     END as cycle_length
--   FROM survival_base s
--   LEFT JOIN individual_survival i ON s.user_id = i.user_id
--   LEFT JOIN robust_bounds r ON s.user_id = r.user_id  -- robust_bounds 조인 추가
-- ),

-- /* 위 로직 엉키지 않게 신규 유저 CTE 따로 생성. 기구매자의 대푯값 적용 */
-- new_user_stats as (
--     select 
--       user_id
--       ,days_since_last_purchase
--       ,Null as current_trailing_term
--       ,Null as prev_trailing_term
--       ,Null as cycle_stddev
--       ,Null as modified_zscore
--       ,age_group
--       ,gender
--       ,Null as demographic_churn_rate
--       ,Null as churn_flag
--       ,survival_prob
--       ,predicted_survival_time
--     ,CASE 
--       WHEN survival_prob <= 0.2 THEN 'High-Risk'
--       WHEN survival_prob <= 0.5 THEN 'Medium-Risk'
--       WHEN survival_prob <= 0.8 THEN 'Low-Risk'
--       ELSE 'Safe'
--     END AS risk_level,
--     "신규" as cycle_length
--     ,pop_mean, gender_mean
--     from(
--       select n.user_id, days_since_last_purchase,age_group, gender, 
--         0.7 * EXP(-n.days_since_last_purchase / t.pop_mean) +
--         0.3 * COALESCE(
--           (SELECT cumulative_survival_prob 
--           FROM km_estimate k 
--           WHERE k.age_group = n.age_group 
--           AND k.gender = n.gender
--           AND k.time_point = FLOOR(n.days_since_last_purchase / 7) * 7), 1 )as survival_prob,
--         Greatest(t.pop_mean * LN(0.5) - n.days_since_last_purchase) as predicted_survival_time
--         ,t.pop_mean
--         ,(SELECT cumulative_survival_prob 
--           FROM km_estimate k 
--           WHERE k.age_group = n.age_group 
--           AND k.gender = n.gender
--           AND k.time_point = FLOOR(n.days_since_last_purchase / 7) * 7) as gender_mean
--       from (
--         SELECT user_id, order_dt as latest_order_dt, date_diff(current_date(), order_dt, day) as days_since_last_purchase ,gender,
--           CAST(FLOOR((EXTRACT(YEAR FROM CURRENT_DATE()) - SAFE_CAST(birth_year AS INT64)) / 10) * 10 AS STRING) as age_group
--         FROM base_data b
--         WHERE not exists (
--           select user_id
--           from user_stats u
--           where u.user_id = b.user_id
--           ) 
--           and recency_rank = 1
--         ) n
--       cross join (
--         select distinct pop_mean
--         from term_stats
--       ) t
--     ))

-- SELECT * FROM new_user_stats
-- SELECT * FROM term_stats
-- )