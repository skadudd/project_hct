## 리커시브 대신 1,2,3 번갈아 가면서 row_number 매김

WITH OrderedEvents AS (
  SELECT 
    Airbridge_Device_Id,
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M", PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%S%Ez", Event_Datetime)) AS formatted_event_datetime,
    Event_Value,
    ROW_NUMBER() OVER (PARTITION BY Airbridge_Device_Id ORDER BY PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%S%Ez", Event_Datetime)) AS seq_num
  FROM `ballosodeuk.airbridge_lake.app_2024` 
  WHERE 
    Event_Date BETWEEN "2024-04-15" AND "2024-05-05" AND
    Event_Category = "view_get__reward_done (App)" AND 
    Event_Label = "소득받기" AND 
    Event_Action = "617"
)
SELECT 
  Airbridge_Device_Id,
  formatted_event_datetime,
  Event_Value,
  MOD((seq_num - 1) , 3) + 1 AS cycle_num
FROM OrderedEvents
ORDER BY formatted_event_datetime;
