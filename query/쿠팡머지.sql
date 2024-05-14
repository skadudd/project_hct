WITH activity AS (
    SELECT 
        ID, 
        Event_Category,

        CASE 
            WHEN Event_Category = 'view_get__ch_join_done (App)' THEN Detail.Action
            -- WHEN Event_Category = 'view_get__page (App)' THEN Detail.Label
            WHEN Event_Category = 'tap_go__bf_list (App)' THEN Detail.Action
            WHEN Event_Category = 'view_get__reward_done (App)' AND Label = '소득받기' THEN Detail.Action
            WHEN Event_Category = 'view_get__reward_done (App)' AND Action = '쿠팡' THEN Detail.Action
            WHEN Event_Category = 'Ad Click (App)' THEN Detail.Action
            ELSE NULL
        END AS details,
        Detail.Value, 
        Detail.Count
    FROM 
        `ballosodeuk.airbridge_mart.key_activity`,
        UNNEST(Detail) AS Detail
    WHERE 
        Event_Date BETWEEN "2024-04-03" AND "2024-05-09"
),

aggregated_activity AS (
    SELECT 
        ID, 
        CONCAT(IFNULL(Event_Category, ''), '_', IFNULL(details, '')) AS event_detail,
        SUM(Value) AS Value, 
        SUM(Count) AS Count
    FROM 
        activity
    GROUP BY 
        ID, Event_Category, details
),

coupang AS(
    SELECT *
    FROM `ballosodeuk.airbridge_mart.coupang_0403-0501`
),

filtered_aggregated_activity AS (
    SELECT 
        a.ID,
        a.event_detail,
        a.Value,
        a.Count
    FROM 
        aggregated_activity a
    JOIN 
        coupang c 
    ON 
        a.ID = c.ID
)

SELECT 
    * 
FROM 
    filtered_aggregated_activity
