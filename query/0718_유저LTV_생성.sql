# 유저의 피초대횟수, 재산 변동성 까지 포함할 것.
DECLARE FIN_DATE DATE;
DECLARE CP_FIN_DATE DATE;
SET FIN_DATE =  DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY);
SET CP_FIN_DATE =  DATE_SUB(CURRENT_DATE(),INTERVAL 2 DAY);

with user_app AS (
  SELECT 
  FROM(SELECT 
    u.User_ID, max(d.datetime) as Joined_Time, max(d.contributionMargin) as Cum_Cash, Max(d.score) as Cur_Cash, max(d.scheduleID) as Mkt_Agreed, 
    Max(d.sharedChannel) as Invicode_Owned, max(d.achievementID) as Invicode_Typed,  Event_Category, d.Label, d.Action, Event_Date,
    SUM(CASE 
      # 재화 지표 정리
      WHEN Event_Category = "view_get__reward_done (App)" AND d.Action = "보너스봉투" THEN ABS(d.Value) - 20
      WHEN Event_Category IN ("Order Complete (App)", "Order Cancel (App)") THEN round(d.Value * 0.044)
      ELSE d.Value
    END) AS Value, Count(*) AS Count
  FROM
      `ballosodeuk.airbridge_mart.app_keyfeatures` ,UNNEST(e) as e, UNNEST(d) as d, UNNEST(u) as u
  WHERE 
    (Event_Date Between FIN_DATE AND FIN_DATE)
    AND Event_Category IN (
    'Open (App)', 'Sign-in (App)', 'view_get__reward_done (App)', 'tap_get__reward_done (App)' , 'Ad Click (App)', 'Order Complete (App)', 'Order Cancel (App)', 'Spend Credits (App)')
  GROUP BY
    u.User_ID, Event_Date, Event_Category, d.Label, d.Action) AS raw
)

,user_app_agg (
  select * 
  from user_app
  group by 
)

,user_coupang AS (
SELECT subParam, Event_Date,sum(quantity) as quantity , sum(gmv) as gmv, sum(commission) as commission
FROM(
    select subParam, date AS Event_Date, quantity, gmv, commission
    from `ballosodeuk.external_mart.cpDynamic_orders`
    WHERE LENGTH(subParam)>5 AND date = CP_FIN_DATE
    UNION ALL
    select subParam, orderDate AS Event_Date, quantity, gmv, commission
    from `ballosodeuk.external_mart.cpDynamic_cancels`
    WHERE LENGTH(subParam)>5 AND orderDate = CP_FIN_DATE
    UNION ALL
    select subParam, date AS Event_Date, quantity, gmv, commission
    from `ballosodeuk.external_mart.reco_orders`
    WHERE LENGTH(subParam)>5 AND date = CP_FIN_DATE
    UNION ALL
    select subParam, orderDate AS Event_Date, quantity, gmv, commission
    from `ballosodeuk.external_mart.reco_cancels`
    WHERE LENGTH(subParam)>5 AND orderDate = CP_FIN_DATE
    ) AS raw 
GROUP BY subParam, Event_Date 
)

,user_merged AS(
    select * from user_app
    
)
 select * from user_coupang


select * from user_app