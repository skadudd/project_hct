with daily_net_amount as 
    (select 
        register_dt, member_no, sum(coalesce(earn,0) - coalesce(cancel,0)) as num
    from
        (SELECT 
            register_dt, member_no,
                CASE WHEN accumulation_status IN ('취소로 인한 지급','지급') THEN amt END as earn,
                CASE WHEN accumulation_status IN ('차감') THEN amt END as cancel
        FROM ballosodeuk.dw.fact_shopby_reward
        WHERE reason != "상품 결제 사용 차감"
        )
    group by register_dt, member_no)

,exception_case as (
  select register_dt, user_id, sum(amt) as amt, "소멸" as reason
  from 
    (select register_dt, a.member_no, wk_id as user_id, amt, reason
    from ballosodeuk.dw.fact_shopby_reward a
    inner join ballosodeuk.dw.dim_shopby_member b on a.member_no = b.member_no
    where reason ="유효기간 만료")
  group by register_dt, user_id
)

,exception_case_2 as (
  select register_dt, user_id, sum(amt) as amt, "교환권" as reason
  from
    (select register_dt, a.member_no, wk_id as user_id, amt
    from ballosodeuk.dw.fact_shopby_reward a
    inner join ballosodeuk.dw.dim_shopby_member b on a.member_no = b.member_no
    where 1=1
      and reason = "운영자 지급" 
      and reason_detail like "%교환%" or reason_detail in("쇼핑지원금 상품권 적립","쇼핑지원금 교환권 적립","쇼핑지원금 환전","쇼핑지원금 5,000원 교환 쿠폰","쇼핑지원금 전환"))
  group by register_dt, user_id
)

,cash_case as (
  select user_id, current_cash
  from ballosodeuk.dw.dim_airbridge_member
)

,cumulative_amount as (
    select register_dt, member_no, num as daily_net_amount,
        sum(num) over (
            partition by member_no
            order by register_dt
            rows between unbounded preceding and current row
        ) as cummulative_amount
    from daily_net_amount
)

,merged_ as 
    (select b.wk_id as user_id, gender, a.member_no,
      cast(floor(
        DATE_DIFF(
          DATE(FORMAT_DATE('%Y-01-01', CURRENT_DATE())), -- 현재 년도의 1월 1일
          DATE(SAFE_CAST(birth_year AS INT64), 1, 1),    -- birth_year의 1월 1일
          YEAR  -- 년 단위로 차이 계산
        ) / 10) * 10 as int64) as age
        ,a.* except(member_no)
    from cumulative_amount a
    inner join ballosodeuk.dw.dim_shopby_member b on a.member_no = b.member_no
    order by user_id, register_dt
    )

,merged as 
  (select register_dt, user_id,member_no, gender,age,
    lag(cummulative_amount) over (partition by user_id order by register_dt asc) as pre_shoji,
    cummulative_amount as current_shoji,
    lead(cummulative_amount,1) over (partition by user_id order by register_dt asc) as post_shoji_1,
    lead(cummulative_amount,2) over (partition by user_id order by register_dt asc) as post_shoji_2,
    lead(cummulative_amount,3) over (partition by user_id order by register_dt asc) as post_shoji_3,
    lead(cummulative_amount,4) over (partition by user_id order by register_dt asc) as post_shoji_4,
    lead(cummulative_amount,5) over (partition by user_id order by register_dt asc) as post_shoji_5,
    lead(cummulative_amount,6) over (partition by user_id order by register_dt asc) as post_shoji_6,
    lead(cummulative_amount,7) over (partition by user_id order by register_dt asc) as post_shoji_7,
    lead(cummulative_amount,8) over (partition by user_id order by register_dt asc) as post_shoji_8,
    lead(cummulative_amount,9) over (partition by user_id order by register_dt asc) as post_shoji_9,
    lead(cummulative_amount,10) over (partition by user_id order by register_dt asc) as post_shoji_10,
    lead(cummulative_amount,11) over (partition by user_id order by register_dt asc) as post_shoji_11,
    lead(cummulative_amount,12) over (partition by user_id order by register_dt asc) as post_shoji_12,
    lead(cummulative_amount,13) over (partition by user_id order by register_dt asc) as post_shoji_13
    
  from merged_ )

,df as 
  (select 
    a.register_dt, a.user_id, member_no, gender,age, pre_shoji, current_shoji
    ,cast(current_cash as int64) + COALESCE(sum(d.amt) over (partition by a.user_id) * 2, 0) as pre_cash
    ,post_shoji_1, post_shoji_2, post_shoji_3, post_shoji_4, post_shoji_5, post_shoji_6, post_shoji_7, post_shoji_8, post_shoji_9, post_shoji_10, post_shoji_11, post_shoji_12, post_shoji_13
    ,c.amt as burnt, d.amt as exchange, d.amt * 2 as exchange_cash_rate
  from merged a
  left join exception_case c on c.user_id = a.user_id and a.register_dt = c.register_dt
  left join exception_case_2 d on d.user_id = a.user_id and a.register_dt = d.register_dt
  left join cash_case e on e.user_id = a.user_id
  )

select 
  register_dt, user_id, member_no, gender, age          
  ,pre_cash - COALESCE(SUM(exchange_cash_rate) OVER (
  PARTITION BY member_no
  ORDER BY register_dt
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 0) + coalesce(exchange_cash_rate,0) as pre_cash
  ,pre_cash - COALESCE(SUM(exchange_cash_rate) OVER (
  PARTITION BY member_no
  ORDER BY register_dt
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 0) as current_cash
  ,* except (register_dt, user_id, member_no, pre_cash)
from df
