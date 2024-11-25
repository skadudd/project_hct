# 변수 및 가중치 상수 선언
DECLARE start_date DATE;
DECLARE end_date DATE;
DECLARE shortcut_avg_commission_rate FLOAT64;
DECLARE network_ad_click_avg_revenue FLOAT64;
DECLARE quiz_goldrain_revenue FLOAT64; # 배수 / reward 금액의 30배 (9/1 ~ 9/18 기준 오차 0%)
DECLARE quiz_wisebricks_revenue FLOAT64; # 배수 / reward 금액의 17배 (9/11 ~ 9/18 기준 오차 4%)
DECLARE quiz_jamong_revenue FLOAT64; # 배수 / reward 금액의 배 매출 데이터를 안줌 ;; 일단 평균치 적용
DECLARE shopping_commission FLOAT64;
set start_date = "2024-10-05";
set end_date = "2024-10-10";
-- set end_date = date_sub(start_date,)
set shortcut_avg_commission_rate = 0.046;
set network_ad_click_avg_revenue = 7.2; 
set quiz_goldrain_revenue = 30; 
set quiz_wisebricks_revenue = 17; 
set quiz_jamong_revenue = 23; 
set shopping_commission = 0.15;
#9/1 ~ 9/10 평균 기준 


# 메인 테이블 정의
with raw as  (
  select 
    Event_Datetime, Airbridge_Device_ID, min(User_ID) as User_ID ,min(Platform) as Platform, max(cum_reward) as cum_reward, 
    max(date(reg_dttm)) as reg_dttm,
    min(Event_Date) as Event_Date, min(Event_Category) as Event_Category, 
    min(Event_Label) as Event_Label, min(Event_Action) as Event_Action,
    min(campaignType) as campaignType, min(campaignName) as campaignName, min(commission) as commission, min(Event_Value) as Event_Value,
  from(
    select 
      Event_Date, Airbridge_Device_ID, User_ID, Platform, e.Event_Datetime, Event_Category, e.Event_Label, e.Event_Action, e.Event_Value, e.Semantic_Event_Properties, e.Custom_Event_Properties,
      REPLACE(JSON_EXTRACT(Custom_Event_Properties, '$.campaignType'), '"', '') AS campaignType ,
      REPLACE(JSON_EXTRACT(Custom_Event_Properties, '$.campaignName'), '"', '') AS campaignName ,
      CAST(REPLACE(JSON_EXTRACT(Custom_Event_Properties, '$.commission'), '"', '') AS FLOAT64) AS commission ,
      SUBSTR(JSON_VALUE(Semantic_Event_Properties, '$.datetime'),1,10) as reg_dttm,
      CAST(REPLACE(JSON_EXTRACT(Semantic_Event_Properties, '$.contributionMargin'), '"', '') AS FLOAT64) AS cum_reward
    FROM 
      `ballosodeuk.airbridge_warehouse.user_event_log`, UNNEST(event_detail) as e
    WHERE 
      Event_Date between start_date and end_date
      and Event_Category in 
        ('view_get__reward_done (App)','view_get__lc_reward_done (App)', 'Ad Click (App)', 'Sign-in (App)')) as a
  where Event_Category in 
    ('Open (App)', 'Deeplink Open (App)', 'Sign-in (App)','view_get__lc_reward_done (App)')
    or (Event_Category = 'view_get__reward_done (App)' and Event_Label in ('퀴즈','오퍼월','매일버튼누르기챌린지'))
    or (Event_Category = 'Ad Click (App)' and Event_Action not in ('스페셜 퀴즈 풀기', '최저가 확인하고 캐시 받기', '최저가 상품 구경하기', '버튼 누르기'))
    
  group by Event_Datetime, Airbridge_Device_ID
  )

,contents as ( # -- 보상 테이블 join 하기 위함
  select User_ID, Event_Action, sum(Contents_Cost) as Contents_Cost, Sum(Contents_Frequency) as Contents_Frequency, 
  from
  (
    select User_ID, Event_Action, sum(Event_Value) as Contents_Cost, count(*) as Contents_Frequency
    from
      (
      select 
        Airbridge_Device_ID, User_ID, Event_Category, Event_Label,
          case 
            when Event_Category = 'view_get__reward_done (App)' and Event_Label = '소득받기' then '챌린지보상'
            when Event_Category = 'view_get__reward_done (App)' and Event_Label = '매일버튼누르기챌린지' then '버튼누르기'
            when Event_Category = 'view_get__reward_done (App)' and Event_Label = '빈캐시' and Event_Action = '빈캐시' then '기본걸음적립'
            else Event_Action end as Event_Action,
          case
            when Event_Category = 'view_get__reward_done (App)' and Event_Action = '보너스봉투' then Event_Value -20
            else Event_Value end as Event_Value
      from `ballosodeuk.airbridge_warehouse.user_event_log` , UNNEST(event_detail) as e
      WHERE 
        Event_Date between start_date and end_date
        and Event_Category in 
          ('view_get__reward_done (App)','view_get__lc_reward_done (App)', 'tap_get__reward_done (App)'))
    group by User_ID, Event_Action
      )
  group by User_ID, Event_Action
  )

,contents_table as (  
  SELECT *  
  FROM contents
  PIVOT (
    SUM(Contents_Cost) AS Cost,
    SUM(Contents_Frequency) AS Freq
    FOR Event_Action IN (
      '기본걸음적립', '버튼누르고바로지급', '와우회원설정', '쿠팡와우회원설정', '쿠팡에서추가', '쿠팡',
      '챌린지보상', '보너스봉투', '가위바위보', '네트워크_애드파이_영상광고', '자몽랩',
      '애드팝콘', '챌린지둘러보기', '와이즈브릭스', '골드레인', '친구초대입력', '아바티', 
      '박터뜨리기', '핀크럭스', '지원금보상'
    )
  )
)

# 유저프로퍼티 테이블 정의 및 데이터 내 최신 값으로 보간
,user_properties AS (
  SELECT 
    Event_Date, 
    User_ID, 
    MAX(Airbridge_Device_ID) AS Airbridge_Device_ID,
    MAX(cum_reward) AS cum_reward,
    MIN(cum_reward) AS cum_reward_min,
    MAX(DATE(reg_dttm)) AS reg_dttm
  FROM raw
  WHERE Event_Category = 'Sign-in (App)' and User_ID not like 'IU_%'
  GROUP BY Event_Date, User_ID
),

user_properties_filled AS (
  SELECT 
    Event_Date,
    User_ID,
    Airbridge_Device_ID,
    -- 누락된 cum_reward 값을 가장 최신의 비-null 값으로 채우기
    COALESCE(cum_reward, 
             LAST_VALUE(cum_reward IGNORE NULLS) OVER (PARTITION BY User_ID ORDER BY Event_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS cum_reward,
    -- 누락된 cum_reward 값을 가장 최신의 비-null 값으로 채우기
    COALESCE(cum_reward_min, 
             LAST_VALUE(cum_reward_min IGNORE NULLS) OVER (PARTITION BY User_ID ORDER BY Event_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS cum_reward_min,
    -- 누락된 reg_dttm 값을 가장 최신의 비-null 값으로 채우기
    COALESCE(reg_dttm, 
             LAST_VALUE(reg_dttm IGNORE NULLS) OVER (PARTITION BY User_ID ORDER BY Event_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS reg_dttm
  FROM user_properties
)

# 외부 매출 및 오퍼월 테이블 정의
# 애드팝콘 
,adpopcorn as(
  select Date(event_date) as Event_Date, 
  case
    when platform = 'aos' then 'Android'
    when platform = 'ios' then 'iOS' END as Platform, 
  TotalRevenue as Total_Revenue, Offerwall_Revenue, RCPM_Revenue, ContentsClick_Revenue
  from `ballosodeuk.airbridge_warehouse.offerwall_adpopcorn_optincome`
  where Date(event_date) between start_date and end_date
)
# 아바티
,avati as(
  select Date(Event_Date) as Event_Date, 
  case
    when platform = 'aos' then 'Android'
    when platform = 'ios' then 'iOS' END as Platform,
  totalRevenue as Total_Revenue, totalExpense
  from `ballosodeuk.airbridge_warehouse.offerwall_avati_pnl`
  where Date(Event_Date) between start_date and end_date
)
# 핀크럭스
,pincrux as( # 로그데이터 수집 정보와 오차율 상당히 낮음. 
  select Date(event_date) as Event_Date, platform as Platform, app_nm, click_cnt, complete_cnt, commission as Total_Revenue
  from `ballosodeuk.airbridge_warehouse.offerwall_pincrux_revenue`
  where Date(event_date) between start_date and end_date
)

# 애드팝콘 아바티 머지
,adpop_avaty as (
  select 'view_get__reward_done (App)' as Event_Category, '애드팝콘' as Event_Action, Event_Date, Platform, Total_Revenue
  from adpopcorn
  union all
  select 'view_get__reward_done (App)' as Event_Category, '아바티' as Event_Action, Event_Date, Platform, Total_Revenue
  from avati
)

## -- 역산해야 하는 매체들 매출 정리 -- ##
,weighted_revenue_offerwall as ( # -> 오퍼월
  select 
    rw.Event_Date, rw.Platform, rw.User_ID, rw.Event_Category, rw.Event_Action, rw.Event_Value, 
    sum(rw.Event_Value) over (partition by rw.Event_Date, rw.Platform, rw.Event_Action) as Daily_Total_Value, 
    rv.Total_Revenue,
    case 
      when rw.Event_Action in ('애드팝콘', '아바티') then round(rw.Event_Value / (sum(rw.Event_Value) over (partition by rw.Event_Date, rw.Platform, rw.Event_Action)) * rv.Total_Revenue, 2)
      when rw.Event_Action in ('와이즈브릭스') then round(rw.Event_Value * quiz_wisebricks_revenue,2)
      when rw.Event_Action in ('골드레인') then round(rw.Event_Value * quiz_goldrain_revenue,2)
      when rw.Event_Action in ('자몽랩') then round(rw.Event_Value * quiz_jamong_revenue,2)
    end as Weighted_Rev, rw.Event_Count_check as Event_Count_check
  from(
    select Event_Date, Platform, User_ID, Event_Category, Event_Action, sum(Event_Value) as Event_Value, count(*) as Event_Count_check
    from raw
    where Event_Category in ('view_get__reward_done (App)')
    group by Event_Date, Platform, User_ID, Event_Category, Event_Action) rw
  left join adpop_avaty rv on rv.Event_Category = rw.Event_Category and rv.Event_Action = rw.Event_Action and rv.Event_Date = rw.Event_Date and rv.Platform = rw.Platform)

,weighted_revenue_network as ( # -> 네트워크 광고 
  select Event_Date, Platform, User_ID, Event_Category, Event_Action, Event_Label, count(*) * network_ad_click_avg_revenue as Network_Revenue, count(*) as Event_Count
  from raw
  where Event_Category in ('Ad Click (App)')
  group by Event_Date, Platform, User_ID, Event_Category, Event_Label, Event_Action)

,order_complete as (
    select Event_Date, Event_Datetime, User_ID, Airbridge_Device_ID, Platform, Event_Category, Event_Label, Event_Action, price as Event_Value
    from `ballosodeuk.airbridge_warehouse.order_event`
    where 
        Date(Event_Date) between start_date and end_date and Event_Category = "Order Complete (App)" and Event_Action not like "^PAY"
        and User_ID not like 'IU_%' and User_ID is not Null
)

## -- 쇼핑 임시 -- ##
,shopping as (
  select Event_Date, User_ID, min(Airbridge_Device_ID) as Airbridge_Device_ID, min(Platform) as Platform, Event_Label, Event_Action, min(Event_Category) as Event_Category, sum(Total_Revenue) as Total_Revenue
  from( 
    select
        Event_Date, Event_Datetime, User_ID, Airbridge_Device_ID, Platform, Event_Category, Event_Action, Event_Label,
        round(sum(Event_Value) * shopping_commission) as Total_Revenue
    from
        order_complete
    where Event_Label = "쇼핑"
    GROUP BY Event_Date, Event_Datetime, User_ID, Airbridge_Device_ID, Platform, Event_Category, Event_Label, Event_Action
  )
  group by Event_Date, User_ID, Event_Label, Event_Action
)

## --  바로가기 -- ##
,shortcut as (
  select Event_Date, User_ID, min(Airbridge_Device_ID) as Airbridge_Device_ID, min(Platform) as Platform, Event_Label, Event_Action, min(Event_Category) as Event_Category, sum(Total_Revenue) as Total_Revenue
  from( 
    select 
      Event_Date, Event_Datetime, User_ID, Airbridge_Device_ID, Platform, Event_Category, Event_Action, 
      CASE 
        WHEN Event_Action in ('balso1sa1','balso1sr1','balso2sr1','balso2sa2') then '바로가기'
        WHEN Event_Action = 'balso1sr2' then '퀴즈쿠팡'
        WHEN Event_Action = 'balso2sa1' then '챌린지인증쿠팡'
      END AS Event_Label, 
      round(sum(Event_Value)  * shortcut_avg_commission_rate) as Total_Revenue
    from
        order_complete
    where Event_Label != "쇼핑"
    GROUP BY Event_Date, Event_Datetime, User_ID, Airbridge_Device_ID, Platform, Event_Category, Event_Label, Event_Action
  )
  group by Event_Date, User_ID, Event_Label, Event_Action
)

## -- 다이나믹 -- ##
, dynamic AS (
    SELECT 
      cast( FORMAT_DATE ('%Y-%m-%d', DATETIME(date)) as Date) as Event_Date, 
      subParam AS User_ID, 
      'Order Complete (App)' AS Event_Category, 
      'dynamic' AS Event_Action, 'coin' As Event_Label,
      SUM(commission) AS Total_Revenue
    FROM `ballosodeuk.external_mart.cpDynamic_orders` 
    WHERE 
      date BETWEEN start_date and end_date
      AND subParam IS NOT NULL
    GROUP BY 
      Event_Date, 
      User_ID, 
      Event_Category, 
      Event_Action
)

,raw_enhanced_external As ( # <- raw에 외부 매출 concat (INL, 다이나믹)
  select 
    Airbridge_Device_ID, User_ID, Platform, cum_reward, reg_dttm, Event_Date, raw.Event_Category, Event_Label, Event_Action, 
    campaignType, campaignName, commission, Event_Value, Null as Rev_Dynamic, Null as Rev_Commerce
  from 
    raw
  union all # 쿠팡 - 다이나믹 추가
  select 
    Null as Airbridge_Device_ID, User_ID, Null as Platform, Null as cum_reward, Null as reg_dttm, Event_Date, 
    Event_Category, Event_Label, Event_Action, Null as campaignType, Null as campaignName, Null as commission, Null as Event_Value, Total_Revenue as Rev_Dynamic, Null as Rev_Commerce
  from 
    dynamic
  union all # 쿠팡 - INL 전체 추가 (010 제외)
  select
    Null as Airbridge_Device_ID, User_ID, Platform, Null as cum_reward, Null as reg_dttm, Event_Date, Event_Category, Event_Label, Event_Action, 
    Null as campaignType, Null as campaignName, Null as commission, Null as Event_Value, Null as Rev_Dynamic, Total_Revenue as Rev_Commerce
  from
    shortcut
  union all # 쇼핑 - 추가 (010 제외)
  select
    Null as Airbridge_Device_ID, User_ID, Platform, Null as cum_reward, Null as reg_dttm, Event_Date, Event_Category, Event_Label, Event_Action, 
    Null as campaignType, Null as campaignName, Null as commission, Null as Event_Value, Null as Rev_Dynamic, Total_Revenue as Rev_Commerce
  from
    shopping
)

# -- 유저 테이블 정리 -- # -> raw 데이터 중 user properties 누락 건 직전 최신 일자의 데이터로 보간
,raw_enhanced_userpropertries as (  
  select 
    User_ID, Event_Date, Max(Platform) as Platform, Event_Category, Event_Label, Event_Action, campaignType, campaignName, 
    max(cum_reward) as cum_reward, min(cum_reward_min) as cum_reward_min, min(reg_dttm) as reg_dttm, sum(commission) as commission,
    sum(Event_Value) as Event_Value, sum(Rev_Dynamic) as Rev_Dynamic, sum(Rev_Commerce) as Rev_Commerce
  from
    (
      select
        case
          when raw.User_ID is Null then p.User_ID
          else raw.User_ID END as User_ID
        ,p.cum_reward, p.cum_reward_min , p.reg_dttm
        ,raw.* Except (Airbridge_Device_ID, User_ID, cum_reward, reg_dttm)
      from raw_enhanced_external as raw
      left join user_properties_filled p on raw.Event_Date = p.Event_Date and raw.User_ID = p.User_ID
    )
  group by User_ID, Event_Date, Event_Category, Event_Label, Event_Action, campaignType, campaignName)


# -- 외부 매출 테이블 조인 -- # -> [[ date / user / properties / cost / revenue ]]
,agg_table as (
  select 
    Event_Date, Platform, User_ID, min(Reg_Dttm) as Reg_Dttm, max(Cum_Reward) as Cum_Reward, min(Cum_Reward) as Cum_Reward_min,
    # 각 매출원 수행 횟수
    count(Rev_Offer_Pincrux) as Cnt_Offer_Pincrux ,
    count(Rev_Offer_etc) as Cnt_Rev_Offer_etc,
    count(Rev_Dynamic) as Cnt_Rev_Dynamic,
    count(Rev_INL_Launcher) as Cnt_Rev_INL_Launcher,
    count(Rev_INL_Quiz) as Cnt_Rev_INL_Quiz,
    count(Rev_INL_Challenge) as Cnt_Rev_INL_Challenge,
    count(Rev_Shopping) as Cnt_Rev_Shopping,
    count(Rev_Network) as Cnt_Rev_Network,
    # 각 매출원 sum
    round(coalesce(sum(Rev_Offer_Pincrux),0),2) as Rev_Offer_Pincrux, 
    round(coalesce(sum(Rev_Offer_etc),0),2) as Rev_Offer_etc, 
    round(coalesce(sum(Rev_Dynamic),0),2) as Rev_Dynamic, 
    round(coalesce(sum(Rev_INL_Launcher),0),2) as Rev_INL_Launcher, 
    round(coalesce(sum(Rev_INL_Quiz),0),2) as Rev_INL_Quiz,
    round(coalesce(sum(Rev_INL_Challenge),0),2) as Rev_INL_Challenge,
    round(coalesce(sum(Rev_Shopping),0),2) as Rev_Shopping,
    round(coalesce(sum(Rev_Network),0),2) as Rev_Network,
    # 총 매출원 sum
    round(
        (COALESCE(SUM(Rev_Offer_Pincrux), 0) + 
        COALESCE(SUM(Rev_Offer_etc), 0) + 
        COALESCE(SUM(Rev_Dynamic), 0) + 
        COALESCE(SUM(Rev_Network), 0) +
        COALESCE(SUM(Rev_Commerce_Total),
        ),2) AS Rev_Total #매출 값 합,

  from
    (
    select # 외부 매체들 조인
      raw_e.*, wt.Weighted_Rev as Rev_Offer_etc,
      case when raw_e.Event_Category = 'Ad Click (App)' then Event_Count * network_ad_click_avg_revenue End as Rev_Network
    from
      (
      select # raw 및 내재 오퍼월 매출 정리
        Event_Date, User_ID, Platform, Event_Category, Event_Label, Event_Action, 
        max(cum_reward) as Cum_Reward, min(cum_reward_min) as Cum_Reward_min, min(reg_dttm) as Reg_Dttm, # -> cum_reward sum > max 로 수정 
        count(*) as Event_Count, sum(commission) as Rev_Offer_Pincrux, sum(Rev_Dynamic) as Rev_Dynamic,
        case when Event_Category = "Order Complete (App)" and Event_Label = "바로가기" then coalesce(sum(Rev_Commerce),0) END as Rev_INL_Launcher,
        case when Event_Category = "Order Complete (App)" and Event_Label = "퀴즈쿠팡" then coalesce(sum(Rev_Commerce),0) END as Rev_INL_Quiz,
        case when Event_Category = "Order Complete (App)" and Event_Label = "챌린지인증쿠팡" then coalesce(sum(Rev_Commerce),0) END as Rev_INL_Challenge,
        case when Event_Category = "Order Complete (App)" and Event_Label = "쇼핑" then coalesce(sum(Rev_Commerce),0) END as Rev_Shopping,
        sum(Rev_Commerce) as Rev_Commerce_Total

      from 
        raw_enhanced_userpropertries
      group by 
        Event_Date, User_ID, Platform, Event_Category, Event_Label, Event_Action
      ) raw_e
    left join # 가중 평균 매출 추가w
      weighted_revenue_offerwall as wt 
        on wt.Event_Date = raw_e.Event_Date and wt.Platform = raw_e.Platform and wt.User_ID = raw_e.User_ID and wt.Event_Category = raw_e.Event_Category and wt.Event_Action = raw_e.Event_Action
    )
  where Event_Category != 'Sign-in (App)'
  group by Event_Date, Platform, User_ID
)

# 검증
-- select * from agg_table
-- select sum(Total_Revenue) from shortcut group by Event_Action
-- select * from raw_enhanced_userpropertries
-- -- 1. 기본 데이터 준비 및 유저 프로퍼티 보간

,base_data AS (
  SELECT
    Event_Date, User_ID,
    COALESCE(Cum_Reward, 
      LAST_VALUE(Cum_Reward IGNORE NULLS) OVER (PARTITION BY User_ID ORDER BY Event_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS Cum_Reward,
    COALESCE(Cum_Reward_min, 
      LAST_VALUE(Cum_Reward_min IGNORE NULLS) OVER (PARTITION BY User_ID ORDER BY Event_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS Cum_Reward_min,
    COALESCE(Reg_Dttm, 
      LAST_VALUE(Reg_Dttm IGNORE NULLS) OVER (PARTITION BY User_ID ORDER BY Event_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS Reg_Dttm,
    DATE_DIFF(
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DATE(Reg_Dttm), DAY
    ) AS Cum_Lifetime,
    COALESCE(Platform, 
      LAST_VALUE(Platform IGNORE NULLS) OVER (PARTITION BY User_ID ORDER BY Event_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS Platform,
    * except (Event_Date, User_ID,Cum_Reward,Cum_Reward_min,Reg_Dttm, Platform)
  FROM agg_table
  WHERE Rev_Total > 0
)

#### -------------- 여기까지 일일 업데이트 ------------------------ ####


-- 2. 피쳐 생성 및 콘텐츠 성향 묶기. RFM 재료 준비 테이블
-- ,feature_data AS (
--   SELECT
--     *,
--     -- DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DATE(Reg_Dttm), DAY) AS Cum_Lifetime,
--     CASE WHEN Rev_Offer_Pincrux > 0 OR Rev_Offer_etc > 0 THEN 1 ELSE 0 END AS Count_Offerwall,
--     CASE WHEN Rev_Network > 0 THEN 1 ELSE 0 END AS Count_Network,
--     CASE WHEN Rev_INL_Launcher > 0 THEN 1 ELSE 0 END AS Count_INL_Launcher,
--     CASE WHEN Rev_INL_Quiz > 0 THEN 1 ELSE 0 END AS Count_INL_Quiz,
--     CASE WHEN Rev_INL_Challenge > 0 THEN 1 ELSE 0 END AS Count_INL_Challenge,
--     CASE WHEN Rev_Dynamic > 0 THEN 1 ELSE 0 END AS Count_Dynamic,
--     RANK() OVER (PARTITION BY User_ID ORDER BY Event_Date ASC) AS Ranks
--   FROM base_data
-- ),

-- 3. RFM 피쳐 계산하는 테이블
rfm_features AS (
  SELECT 
    Event_Date, User_ID, MAX(Platform) AS Platform, 
    MIN(Reg_Dttm) AS Reg_Dttm, MAX(Cum_Lifetime) AS Cum_Lifetime,
    MAX(Cum_Reward) AS Cum_Reward, MIN(Cum_Reward_min) AS Cum_Reward_min,
    SUM(Rev_Offer_Pincrux) AS Rev_Offer_Pincrux,
    SUM(Rev_Offer_etc) AS Rev_Offer_etc,
    SUM(Rev_Network) AS Rev_Network,
    SUM(Rev_INL_Launcher) AS Rev_INL_Launcher,
    SUM(Rev_INL_Challenge) AS Rev_INL_Challenge,
    SUM(Rev_INL_Quiz) AS Rev_INL_Quiz,
    SUM(Rev_Dynamic) AS Rev_Dynamic,
    MAX(CASE WHEN Count_Offerwall > 0 THEN Event_Date END) AS Recency_Offerwall,
    MAX(CASE WHEN Count_Network > 0 THEN Event_Date END) AS Recency_Network,
    MAX(CASE WHEN Count_INL_Launcher > 0 THEN Event_Date END) AS Recency_INL_Launcher,
    MAX(CASE WHEN Count_INL_Challenge > 0 THEN Event_Date END) AS Recency_INL_Challenge,
    MAX(CASE WHEN Count_INL_Quiz > 0 THEN Event_Date END) AS Recency_INL_Quiz,
    MAX(CASE WHEN Count_Dynamic > 0 THEN Event_Date END) AS Recency_Dynamic,
    MAX(Event_Date) AS Recncy_Total,
    MAX(CASE WHEN Count_Offerwall > 0 OR Count_INL_Launcher > 0 OR Count_INL_Challenge > 0 OR Count_INL_Quiz > 0 OR Count_Dynamic > 0 THEN Event_Date END) AS Recency_Total_NoAd, 

    
    -- F_u -> 고유 일자로 집계
    COUNT(DISTINCT CASE WHEN Count_Offerwall > 0 THEN Event_Date END) AS Frequency_Offerwall,
    COUNT(DISTINCT CASE WHEN Count_Network > 0 THEN Event_Date END) AS Frequency_Network,
    COUNT(DISTINCT CASE WHEN Count_INL_Launcher > 0 THEN Event_Date END) AS Frequency_INL_Launcher,
    COUNT(DISTINCT CASE WHEN Count_INL_Challenge > 0 THEN Event_Date END) AS Frequency_INL_Challenge,
    COUNT(DISTINCT CASE WHEN Count_INL_Quiz > 0 THEN Event_Date END) AS Frequency_INL_Quiz,
    COUNT(DISTINCT CASE WHEN Count_Dynamic > 0 THEN Event_Date END) AS Frequency_Dynamic,
    -- F_c -> 횟수로 집계
    sum(Cnt_Offer_Pincrux) + sum(Cnt_Rev_Offer_etc) as Frequency_Offerwall_c,
    sum(Cnt_Rev_Network) as Frequency_Network_c,
    sum(Cnt_Rev_INL_Launcher) as Frequency_INL_Launcher_c,
    sum(Cnt_Rev_INL_Challenge) as Frequency_INL_Challenge_c,
    sum(Cnt_Rev_INL_Quiz) as Frequency_INL_QUiz_c,
    sum(Cnt_Rev_Dynamic) as Frequency_Dynamic_c
  FROM base_data
  GROUP BY Event_Date, User_ID
)


-- select *
-- from rfm_features

select *
from feature_data

# 15일, 여기까지 체크