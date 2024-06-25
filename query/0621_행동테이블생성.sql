CREATE OR REPLACE TABLE `ballosodeuk.airbridge_mart.activity_mart`
PARTITION BY Event_Date
CLUSTER BY ID, Event_Category
AS
WITH fact AS (
  SELECT
    Airbridge_Device_ID,ID_Source,User_ID
    Event_Date, Platform, Device_Type, Device_Model, App_Version,
    Event_Category, Event_Action, Event_Label, Event_Value, 
  FROM `ballosodeuk.airbridge_lake.app_2024`
  WHERE Event_Date BETWEEN "2024-03-01" AND "2024-05-12"
),
grp AS (
  SELECT 
    Event_Date, ID, ID_Source, Event_Category, Event_Label, Event_Action, SUM(Event_Value) AS Event_Value, COUNT(*) AS cnt
  FROM fact
  GROUP BY Event_Date, ID, ID_Source, Event_Category, Event_Label, Event_Action
),
total_grp AS (
  SELECT 
    Event_Date, ID, ID_Source, Event_Category, SUM(Event_Value) AS Event_Value, COUNT(*) AS Counts,
    ARRAY_AGG(STRUCT(Event_Action AS Action, Event_Label AS Label, cnt AS count, Event_Value as Value)) AS Detail
  FROM grp
  WHERE 
    Event_Category IN (
      'tap_try__bf_coupang_join (App)', 'tap_try__bf_cps_join (App)', 
      'tap_try__rsp_join (App)', 'tap_try__bf_bongtu_open (App)', 
      'tap_go__bf_list (App)', 'tap_get__reward_done (App)', 
      'view_get__reward_done (App)', 'view_get__ch_join_done (App)', 
      'view_get__page (App)', 'Ad Click (App)', 'Spend Credits (App)'
    )
  GROUP BY Event_Date, ID, ID_Source, Event_Category
)
SELECT * FROM total_grp;