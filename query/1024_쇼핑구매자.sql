select User_ID, sum(Event_Value) as Rev, count(*) as Cnt
from `ballosodeuk.airbridge_warehouse.user_event_log`,unnest(event_detail)
where 
  Event_Date between '2024-10-18' and '2024-10-23' and
  Event_Category = 'Order Complete (App)' and Event_Label = '쇼핑'
group by User_ID