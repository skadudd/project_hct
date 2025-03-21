DECLARE end_date DATE;
SET end_date = CURRENT_DATE() - 1;

CREATE OR REPLACE TABLE ballosodeuk.ynam.temp_base_activity AS (
  WITH 
  -- 전체 사용자 데이터: 기간 조건만 적용 (earn_reward 값과 상관없이)
  base_data AS (
    SELECT 
      register_dt,
      case when length(user_id) < 1 then member_no else user_id end as user_id, -- 수정
      member_no,
      signout_dt,
      age,
      gender,
      earn_reward,
      earn_exchange,
      spend_use
    FROM ballosodeuk.dm.agg_user_cash_daily
    WHERE register_dt BETWEEN DATE("2024-10-01") AND DATE(end_date)
  ),
  -- 전체 사용자 리스트 (중복 제거)
  base_info AS (
    SELECT DISTINCT user_id, gender, age, signout_dt
    FROM base_data
  ),
  -- 활동 체크용 데이터: earn_reward > 0 인 행만 활용
  filtered_activity AS ( ##### 생존을 체크하는 피쳐 셀렉 ########
      SELECT *
      FROM base_data
      WHERE COALESCE(earn_reward, 0) != 0 
        OR COALESCE(earn_exchange, 0) != 0 
        OR COALESCE(spend_use, 0) != 0
    ),
  activity_data AS (
    SELECT 
      register_dt,
      user_id,
      earn_reward,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY register_dt DESC) AS recency_rank,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY register_dt ASC) AS activity_rank
    FROM filtered_activity
  ),
  segmented_activity_raw AS (
    SELECT 
      ad.*,
      LAG(register_dt) OVER (PARTITION BY user_id ORDER BY register_dt) AS prev_register_dt
    FROM activity_data ad
  ),
  segmented_activity AS (
    SELECT 
      sar.*,
      SUM(
        CASE 
          WHEN prev_register_dt IS NOT NULL 
               AND DATE_DIFF(register_dt, prev_register_dt, DAY) > 30 
          THEN 1 ELSE 0 
        END
      ) OVER (PARTITION BY user_id ORDER BY register_dt ROWS UNBOUNDED PRECEDING) AS segment_id
    FROM segmented_activity_raw sar
  ),
  segmented_activity_ranked AS (
    SELECT 
      *,
      ROW_NUMBER() OVER (PARTITION BY user_id, segment_id ORDER BY register_dt) AS segment_activity_rank
    FROM segmented_activity
  )
  
  -- 최종적으로 전체 사용자(base_info)와 활동 데이터(있는 경우)를 LEFT JOIN
  SELECT 
    bi.user_id,
    bi.gender,
    bi.age,
    bi.signout_dt,
    aa.register_dt,
    aa.recency_rank,
    aa.activity_rank,
    aa.segment_id,
    aa.segment_activity_rank,
    end_date AS cur_date
  FROM base_info bi
  LEFT JOIN (
    SELECT *
    FROM segmented_activity_ranked
  ) aa
    ON bi.user_id = aa.user_id
);

# 전체 유저 이탈 - 2단계
DECLARE end_date DATE;
SET end_date = CURRENT_DATE() - 1;

CREATE OR REPLACE TABLE ballosodeuk.ynam.temp_activity_stats AS (
  WITH current_segment AS (
    SELECT 
      user_id, max(gender) as gender, max(CAST(FLOOR(age/10)*10 AS STRING)) AS age_group, max(signout_dt) as signout_dt,
      MAX(segment_id) AS current_segment_id
    FROM ballosodeuk.ynam.temp_base_activity
    WHERE 1=1
    # cur_date 조건 추가 (250320, jhson)
    AND cur_date = end_date
    GROUP BY user_id
  )

  ,activity_intervals AS (
    SELECT 
      sar.user_id,
      sar.register_dt,
      sar.segment_activity_rank,
      sar.segment_id,
      DATE_DIFF(
        sar.register_dt,
        LEAD(sar.register_dt) OVER 
          (PARTITION BY sar.user_id, sar.segment_id ORDER BY sar.register_dt desc), 
           DAY
      ) AS days_between_activity
    FROM ballosodeuk.ynam.temp_base_activity sar
    WHERE 1=1
    # cur_date 조건 추가 (250320, jhson)
    AND cur_date = end_date
  )
  -- select * from activity_intervals where user_id = "00321906-a9f0-4e5a-83fb-9509b5d4bf76"
  
  ,churn_resurrect AS ( -- 30일 기준 활동 세그먼트 & 세그먼트 별 이탈 및 부활 일자
    select user_id, register_dt, segment_activity_rank, segment_id, days_between_activity, is_churn
      ,case when is_churn is True then max(register_dt) over (partition by user_id, segment_id) else Null end as churn_dt
      ,case when segment_id > 0 then min(register_dt) over (partition by user_id, segment_id) else Null end as resurrect_dt
    from  
      (select user_id, register_dt, segment_activity_rank, segment_id, days_between_activity,
        case when date_diff(end_date, max(register_dt) over (partition by user_id, segment_id) ,DAY) > 30 
          then True else False end as is_churn,
      from activity_intervals)
  )
  ,churn_resurrect_agg as ( -- 유저의 몇번째 생애인지, 최근 이탈, 부활 날짜, 현재 이탈상태
    select 
      a.user_id, segment_cnt, last_seg_first_dt, churn_dt, resurrect_dt
      ,case when churn_dt is Null or resurrect_dt > churn_dt then False else True end as is_churn
      ,case 
          when churn_dt is Null or resurrect_dt > churn_dt then
            DATE_DIFF(DATE(end_date), last_seg_first_dt, DAY) 
          else 
            DATE_DIFF(churn_dt, last_seg_first_dt, DAY)
        end as alive_days
    from(
        select 
          user_id, count(distinct segment_id) as segment_cnt,max(churn_dt) as churn_dt
          ,max(resurrect_dt) as resurrect_dt,
        from churn_resurrect
        group by user_id) a
    left join(
      select user_id, max(seg_first_dt) as last_seg_first_dt
      from 
        (select 
          user_id, first_value(register_dt) over (partition by user_id, segment_id order by register_dt) as seg_first_dt
        from 
          churn_resurrect)
      group by user_id
    ) b on a.user_id = b.user_id
  )
  
  ,user_stats AS (
    SELECT 
      user_id,
      case when sum(coalesce(days_between_activity,0)) is Null then Null
        else STDDEV(days_between_activity) end AS cycle_stddev
    FROM activity_intervals
    GROUP BY user_id
  )
  -- select * from user_stats where user_id = "00321906-a9f0-4e5a-83fb-9509b5d4bf76"

  ,recent_activity AS (
    SELECT 
      ba.user_id,
      ba.register_dt AS latest_activity_dt,
      DATE_DIFF(DATE(end_date), ba.register_dt, DAY) AS days_since_last_activity
    FROM ballosodeuk.ynam.temp_base_activity ba
    JOIN current_segment cs ON ba.user_id = cs.user_id 
    WHERE ba.recency_rank = 1
    
    AND cur_date = end_date
  )
  -- select * from recent_activity where user_id = "00321906-a9f0-4e5a-83fb-9509b5d4bf76"

  ,current_trailing AS (
    SELECT 
      user_id, segment_id, round(avg(days_between_activity),1) as current_trailing_term
    FROM activity_intervals
    GROUP BY user_id, segment_id
    Order by segment_id ASC
  )

  ,seg_stats AS (
    -- 각 사용자별 최대 segment_id를 구합니다.
    SELECT 
      user_id,
      MAX(segment_id) AS max_seg
    FROM current_trailing
    GROUP BY user_id
  ),
  weighted_trailing AS (
    -- 모든 세그먼트를 포함하되, 각 세그먼트별 가중치는 (max_seg - segment_id + 1)/(max_seg + 1) 로 계산합니다.
    SELECT 
      ct.user_id,
      ct.segment_id,
      ct.current_trailing_term,
      (ss.max_seg - ct.segment_id + 1) / (ss.max_seg + 1.0) AS weight
    FROM current_trailing ct
    JOIN seg_stats ss ON ct.user_id = ss.user_id
    -- 여기서는 NULL인 current_trailing_term도 제거하지 않습니다.
  ),
  agg_weighted AS (
    select user_id, 
      -- 만약 total_weight가 0이면(즉, 모든 current_trailing_term이 NULL이면) 결과는 NULL로 반환합니다.
      CASE WHEN total_weight = 0 THEN NULL
          ELSE ROUND(weighted_sum / total_weight, 1)
      END AS weighted_current_trailing_term
    from
      (SELECT 
        user_id,
        -- current_trailing_term이 NULL인 경우 가중치 곱은 0으로 처리하고, 
        -- non-NULL 값에 대해서만 가중치 합계를 계산합니다.
        SUM(CASE WHEN current_trailing_term IS NOT NULL THEN weight * current_trailing_term ELSE 0 END) AS weighted_sum,
        SUM(CASE WHEN current_trailing_term IS NOT NULL THEN weight ELSE 0 END) AS total_weight
      FROM weighted_trailing
      GROUP BY user_id)
  )
  
  ,agg as
    (
      select * except (is_churn), 
        case when 
          signout_dt Is not Null and signout_dt > resurrect_dt then false
          else is_churn end as is_churn 
      from 
      (select 
        c.user_id, gender, age_group,  current_segment_id, 
        case when weighted_current_trailing_term is null then 
        avg(weighted_current_trailing_term) over () else weighted_current_trailing_term end as weighted_current_trailing_term
        ,cycle_stddev, days_since_last_activity, latest_activity_dt,
        segment_cnt, last_seg_first_dt, signout_dt, churn_dt, resurrect_dt, is_churn, alive_days
        ,CASE 
          WHEN signout_dt IS NOT NULL 
              AND signout_dt <= DATE(end_date) THEN 1 
          ELSE 0 
      END AS event_flag,
      end_date as cur_date
      from current_segment c
      left join user_stats us on us.user_id = c.user_id
      left join recent_activity ra on ra.user_id = c.user_id
      left join agg_weighted agg on agg.user_id = c.user_id 
      left join churn_resurrect_agg cagg on cagg.user_id = c.user_id)
    )
    
  select * from agg


);

DECLARE end_date DATE;
SET end_date = CURRENT_DATE() - 1;

CREATE OR REPLACE TABLE ballosodeuk.ynam.refined_sigmoid_survival 

AS (
  with survival_base_ as (
    select * 
    from ballosodeuk.ynam.temp_activity_stats
  )

  -- 생존 기간별 구간 집계
  ,survival_base_aggregated AS (
    SELECT
      FLOOR(s.alive_days / 3) * 3 AS time_point,  -- 3일 단위 구간
      COUNT(s.user_id) AS n_risk,
      SUM(CASE WHEN s.is_churn = TRUE THEN 1 ELSE 0 END) AS n_events,
      ROUND(
        (1 - SAFE_DIVIDE(SUM(CASE WHEN s.is_churn = TRUE THEN 1 ELSE 0 END), COUNT(s.user_id))),
        4
      ) AS base_survival_prob
    FROM survival_base_ s
    GROUP BY FLOOR(s.alive_days / 3) * 3
  )

  -- 방문 주기 통계
  ,term_stats AS (
    SELECT 
      AVG(weighted_current_trailing_term) AS avg_term,
      APPROX_QUANTILES(weighted_current_trailing_term, 100)[OFFSET(50)] AS median_term
    FROM survival_base_
    WHERE weighted_current_trailing_term IS NOT NULL
      AND weighted_current_trailing_term > 0
  )

  -- 기본 데이터 준비 및 시그모이드 파라미터 계산
  ,survival_base as (
    SELECT 
      s.*, 
      agg.base_survival_prob,
      t.avg_term,
      t.median_term,
      
      -- 중간점: 이탈 기준(30일) & 경험적 위험지표인 7일의 +- 7*0.5 range
      -- 선형함수 기반 midpoint 계산 (최소값 보정 포함)
      LEAST(10.5,GREATEST(3.5, 0.3 * weighted_current_trailing_term + 0.5 * alive_days)) AS sigmoid_midpoint,
      
      -- 경험적으로 직접 설정한 기울기 값
      -- 미방문 0일에는 약 100%, 15일에는 50%, 25일에는 10%, 30일에는 0%가 되도록 설정
      -- alive_days 는 이탈이 아닌 판정 상태인 유저의 첫 방문 으로부터 현재까지의 생존 일수.
      -- 따라서 alive_days - days_since_last_activity가 실제 alive_days
      CASE 
        -- 최상위 충성 유저(>90일): 매우 완만한 기울기
        WHEN s.alive_days - s.days_since_last_activity > 90 THEN 0.15
        -- 장기 충성 유저(60-90일): 완만한 기울기
        WHEN s.alive_days - s.days_since_last_activity > 60 THEN 0.18
        -- 안정적인 유저(30-60일): 중간 기울기
        WHEN s.alive_days - s.days_since_last_activity > 30 THEN 0.22
        -- 정착기 유저(21-30일): 약간 높은 기울기
        WHEN s.alive_days - s.days_since_last_activity > 21 THEN 0.25
        -- 중기 유저(14-21일): 높은 기울기
        WHEN s.alive_days - s.days_since_last_activity > 14 THEN 0.28
        -- 적응기 유저(7-14일): 매우 높은 기울기
        WHEN s.alive_days - s.days_since_last_activity > 7 THEN 0.30
        -- 초기 유저(7일 이하): 가장 높은 기울기
        ELSE 0.35
      END AS sigmoid_steepness,
      
      -- 개인화/집단화 균형을 위한 가중치
      0.9 AS w  -- 개인화 가중치 90%로 상향 조정
    FROM survival_base_ s
    LEFT JOIN survival_base_aggregated agg 
      ON agg.time_point = FLOOR(s.alive_days / 3) * 3
    CROSS JOIN term_stats t
  )
  
  -- 개별 유저 생존 확률 계산 (개선된 시그모이드 함수 사용)
  ,individual_survival AS (
    SELECT 
      s.user_id,
      s.age_group,
      s.segment_cnt,
      s.days_since_last_activity,
      s.weighted_current_trailing_term,
      s.latest_activity_dt,
      s.signout_dt,
      s.churn_dt,
      s.resurrect_dt,
      s.alive_days,
      s.is_churn,
      s.event_flag,
      s.w,
      s.sigmoid_steepness,
      s.sigmoid_midpoint,
      s.avg_term,
      s.median_term,
      
      -- 미방문 일수가 0일 때 생존 확률을 1로 처리하는 예외 추가
      CASE 
        -- 1. 이벤트 발생 시: 즉시 churn
        WHEN s.event_flag = 1 THEN 0
        
        -- 2. 30일 이상 미활동: 무조건 churn
        WHEN s.days_since_last_activity >= 30 THEN 0

        WHEN s.days_since_last_activity IS NULL THEN 0
        -- 3. 미방문 0일: 생존 확률 100% (특별 케이스)
        WHEN s.days_since_last_activity = 0 THEN 1.0
        
        ELSE
          -- 4. 1일 ~ 29일 구간: 개선된 시그모이드 함수 적용
          (
            -- 개인화된 시그모이드 생존 함수
            s.w * (1 / (1 + EXP((s.days_since_last_activity - s.sigmoid_midpoint) * s.sigmoid_steepness)))
          ) + (
            -- 집단 기반 생존 함수 (전체 집단 패턴 적용)
            (1 - s.w) * s.base_survival_prob
          )
      END AS survival_prob
    FROM survival_base s
  )
  
  -- 최종 분석 결과
  ,final_analysis as (
    SELECT 
      s.*,
      -- 방문 주기 길이에 따른 유저 분류
      CASE
        WHEN s.weighted_current_trailing_term is Null THEN '신규' 
        WHEN s.weighted_current_trailing_term <= 3 THEN '초단기'
        WHEN s.weighted_current_trailing_term <= 7 THEN '단기'
        WHEN s.weighted_current_trailing_term <= 14 THEN '중기'
        ELSE '장기'
      END AS cycle_length,
      
      -- 생존 기간에 따른 유저 분류
      CASE 
        WHEN s.alive_days is Null then '기이탈 유저'
        WHEN s.alive_days <= 7 THEN '초기 유저'
        WHEN s.alive_days <= 14 THEN '적응기 유저'
        WHEN s.alive_days <= 21 THEN '정착기 유저'
        WHEN s.alive_days <= 30 THEN '안정기 유저'
        WHEN s.alive_days <= 60 THEN '충성 유저'
        WHEN s.alive_days <= 90 THEN '장기 충성 유저'
        ELSE '최상위 충성 유저'
      END AS loyalty_segment
    FROM individual_survival s
  )
  
  -- 최종 결과 반환
  SELECT 
    user_id,
    segment_cnt as lifetime_cnt,
    weighted_current_trailing_term, 
    days_since_last_activity,
    latest_activity_dt, 
    churn_dt, 
    alive_days, 
    resurrect_dt,
    CASE 
      WHEN survival_prob < 0.01 OR survival_prob IS NULL THEN TRUE
      WHEN days_since_last_activity IS NULL THEN TRUE  -- Add this condition
      WHEN latest_activity_dt IS NULL THEN TRUE  -- Add this condition
      ELSE is_churn 
    END AS is_churn,
    w,
    sigmoid_steepness,
    sigmoid_midpoint,
    survival_prob,
    cycle_length,
    loyalty_segment,
    end_date as cur_date
  FROM final_analysis

  -- WHERE alive_days > 0 
  -- and loyalty_segment = "초기 유저"
  -- order by rand()
  -- limit 100
);