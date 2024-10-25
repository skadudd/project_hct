with raw as
  (SELECT User_ID, Event_Date, Event_Category, Event_Action, Event_Value, 
    cast( replace(json_extract(Semantic_Event_Properties, '$.score'),'"','') as int64) as score
  FROM `ballosodeuk.airbridge_warehouse.user_event_log`,unnest(event_detail)
  WHERE Event_Date between "2024-09-01" and "2024-10-22" #5/3 부터 로그인 properties 수집
  and Event_Category in ('Spend Credits (App)', 'Sign-in (App)')
  )

,spend_range_table as  -- -> 소비 구간화
  (
    select User_ID, Value_Range, count(Value_Range) as P_count
    from
      (select 
        User_ID,Event_Date,
        case
          when Event_Value < 1000 then '0-1000'
          when Event_Value >= 1000 and Event_Value < 3000 then '1000-2999'
          when Event_Value >= 3000 and Event_Value < 5000 then '3000-4999'
          when Event_Value >= 5000 and Event_Value < 7000 then '5000-6999'
          when Event_Value >= 7000 and Event_Value < 9000 then '7000-8999'
          when Event_Value >= 9000 and Event_Value < 11000 then '9000-12999'
          when Event_Value >= 11000 and Event_Value <= 12999 then '11000 - 12999'
          when Event_Value >= 13000 and Event_Value <= 14999 then '13000 - 14999'
          when Event_Value >= 15000 and Event_Value <= 16999 then '15000 - 16999'
          when Event_Value >= 17000 and Event_Value <= 18999 then '17000 - 18999'
          when Event_Value >= 19000 and Event_Value <= 20999 then '19000 - 20999'
          when Event_Value >= 21000 and Event_Value <= 22999 then '21000 - 22999'
          when Event_Value >= 23000 and Event_Value <= 24999 then '23000 - 24999'
          when Event_Value >= 25000 and Event_Value <= 26999 then '25000 - 26999'
          when Event_Value >= 27000 and Event_Value <= 28999 then '27000 - 28999'
          when Event_Value >= 29000 and Event_Value <= 30999 then '29000 - 30999'
          when Event_Value >= 31000 and Event_Value <= 32999 then '31000 - 32999'
          when Event_Value >= 33000 and Event_Value <= 34999 then '33000 - 34999'
          when Event_Value >= 35000 and Event_Value <= 36999 then '35000 - 36999'
          when Event_Value >= 37000 and Event_Value <= 38999 then '37000 - 38999'
          when Event_Value >= 39000 and Event_Value <= 40999 then '39000 - 40999'
          else '41000 -' end as Value_Range
      from raw
      where Event_Category = 'Spend Credits (App)')
  group by User_ID, Value_Range)

, filled_range_table AS ( -- -> 각 유저 별 모든 구간을 갖도록 fill
  SELECT u.User_ID, r.Value_Range, COALESCE(s.P_count, 0) AS P_count
  FROM (
    SELECT DISTINCT User_ID 
    FROM spend_range_table) u
  CROSS JOIN (
    SELECT DISTINCT Value_Range 
    FROM spend_range_table) r
  LEFT JOIN spend_range_table s
    ON u.User_ID = s.User_ID AND r.Value_Range = s.Value_Range
)

,pivot_spend_range_table as ( -- -> 구간화 테이블 피봇
  select *
  from filled_range_table
  pivot (
    sum(P_count) for Value_Range in 
    ('0-1000','1000-2999','3000-4999','5000-6999','7000-8999','9000-12999','11000 - 12999','13000 - 14999','15000 - 16999','17000 - 18999',
    '19000 - 20999','21000 - 22999','23000 - 24999','25000 - 26999','27000 - 28999','29000 - 30999','31000 - 32999','33000 - 34999',
    '35000 - 36999','37000 - 38999','39000 - 40999','41000 -')
  )
  order by `0-1000`,`1000-2999`,`3000-4999`,`5000-6999`,`7000-8999`,`9000-12999`,`11000 - 12999`,`13000 - 14999`,`15000 - 16999`,`17000 - 18999`,
    `19000 - 20999`,`21000 - 22999`,`23000 - 24999`,`25000 - 26999`,`27000 - 28999`,`29000 - 30999`,`31000 - 32999`,`33000 - 34999`,
    `35000 - 36999`,`37000 - 38999`,`39000 - 40999`,`41000 -`
)

,total_spend_count_table as ( -- -> 엔트로피 계산을 위해 유저 별 총 구매수 집계
  select User_ID, Sum(P_count) as total_count
  from spend_range_table
  group by User_ID
)

,entropy_calculation as ( -- -> 엔트로피 계산
  select s.User_ID,
    round(-sum((P_count / t.total_count) * (log(P_count / t.total_count) / log(2))),2) as entropy
  from spend_range_table as s
  join total_spend_count_table as t
    on s.User_ID = t.User_ID
  group by s.User_ID
)

-- ,range_table as ( -- ->   
--   select 
--     User_ID, date_diff(max(Event_Date), min(Event_Date), day) as p_range, 
--     count(distinct Event_Date) as re_p, date_diff(max(Event_Date), min(Event_Date), day) / count(distinct Event_Date) as avg_p
--   from raw
--   where Event_Category = 'Spend Credits (App)'
--   group by User_ID)

,result_pivotrange_entropy as ( -- -> 피봇 + 엔트로피  
  select a.User_ID, b.entropy, a.* except(User_ID)
  from pivot_spend_range_table a
  join entropy_calculation b on a.User_ID = b.User_ID)

,avg_spend_ratio as ( -- 당일 로그인 당일 보유 최대 캐시 대비 지출
    select User_ID, case when avg(Spend_Ratio) > 1.0 then 1.0 else avg(Spend_Ratio) end as Mean_Spend_Ratio -- 당일 earning 이 컸던 예외 케이스 처리
    from
      (select -- -> 소수의 Log-in 누락 유저 제외
        Event_Date, User_ID, Spend / COALESCE(Prop, Pre_Prop, Post_Prop) as Spend_Ratio, Spend, COALESCE(Prop, Pre_Prop, Post_Prop) as Prop
      from
        (select -- Log-in 누락 보간
          Event_Date, User_ID, Prop, Spend, 
          LAG(Prop) OVER(PARTITION BY User_ID ORDER BY Event_Date) as Pre_Prop,
          LEAD(Prop) OVER(PARTITION BY User_ID ORDER BY Event_Date) as Post_Prop
        from
          (
            select Event_Date, User_ID, max(score) as Prop, sum(Event_Value) as Spend
            from raw
            where User_ID is not Null
            group by Event_Date, User_ID
            having Spend is not Null
            order by User_ID, Event_Date
          )
        )
      where Prop > 0)
    group by User_ID
      
)


#구매 주기
,spending_gap as (
  SELECT User_ID, Event_Date, LAG(Event_Date) Over (Partition by User_ID ORDER BY Event_Date) as Prev_Event_Date
  FROM
    (
      select User_ID, Event_Date
      from
        (select User_ID, Event_Date
        from raw
        where Event_Category = 'Spend Credits (App)'
        )
      group by User_ID, Event_Date
    )
  )

,avg_spending_intervals as (
  select User_ID, avg(Interval_Days) as Interval_Days
  from
    (
      SELECT User_ID, 
        case 
          when Prev_Event_Date is not Null then Date_Diff(Event_Date, Prev_Event_Date, Day) 
          else 0.0 end as Interval_Days
      FROM spending_gap
    )
  group by User_ID
)


,Spending_Power_Stats as # -> 쿠폰 구매 시 재산 대비 소진율 평균
  (select Mean, 
    Mean - (1.96 * std / sqrt(n)) as Lower_CI,
    Mean + (1.96 * std / sqrt(n)) as Upper_CI,
  from
    (select avg(Mean_Spend_Ratio) as Mean, STDDEV(Mean_Spend_Ratio) as std, COUNT(*) as n
    from avg_spend_ratio)
)

select a.User_ID, b.* except (User_ID), c.Interval_Days, a.* except (User_ID)
from result_pivotrange_entropy a
left join avg_spend_ratio b on a.User_ID = b.User_ID
left join avg_spending_intervals c on c.User_ID = a.User_ID

-- select *
-- from result_pivotrange_entropy