# 유저의 피초대횟수, 재산 변동성 까지 포함할 것.
DECLARE FIN_DATE DATE;
DECLARE CP_FIN_DATE DATE;
SET FIN_DATE =  DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY);
SET CP_FIN_DATE =  DATE_SUB(CURRENT_DATE(),INTERVAL 2 DAY);

with 
user_app AS (
SELECT u.User_ID, d.datetime, d.contributionMargin, d.scheduleID, d.sharedChannel, d.achievementID, d.score ,Event_Category, d.Label, d.Action, d.Value, 
FROM `ballosodeuk.airbridge_mart.app_keyfeatures` ,UNNEST(e) as e, UNNEST(d) as d, UNNEST(u) as u
WHERE 
  (Event_Date Between CP_FIN_DATE AND FIN_DATE)
  AND Event_Category IN (
    'Open (App)', 'Sign-in (App)', 'view_get__reward_done (App)', 'tap_get__reward_done (App)' , 'Ad Click (App)', 'Order Complete (App)', 'Order Cancel (App)', 'Spend Credits (App)'
    )
 )

-- ,user_coupang AS (
-- SELECT subParam, Event_Date,sum(quantity) as quantity , sum(gmv) as gmv, sum(commission) as commission
-- FROM(
--     select subParam, date AS Event_Date, quantity, gmv, commission
--     from `ballosodeuk.external_mart.cpDynamic_orders`
--     WHERE LENGTH(subParam)>5 AND date = CP_FIN_DATE
--     UNION ALL
--     select subParam, orderDate AS Event_Date, quantity, gmv, commission
--     from `ballosodeuk.external_mart.cpDynamic_cancels`
--     WHERE LENGTH(subParam)>5 AND orderDate = CP_FIN_DATE
--     UNION ALL
--     select subParam, date AS Event_Date, quantity, gmv, commission
--     from `ballosodeuk.external_mart.reco_orders`
--     WHERE LENGTH(subParam)>5 AND date = CP_FIN_DATE
--     UNION ALL
--     select subParam, orderDate AS Event_Date, quantity, gmv, commission
--     from `ballosodeuk.external_mart.reco_cancels`
--     WHERE LENGTH(subParam)>5 AND orderDate = CP_FIN_DATE
--     ) AS raw 
-- GROUP BY subParam, Event_Date 
-- ),

-- user_merged AS(
--     select * from user_app
    
-- )
--  select * from user_coupang


select * from user_app