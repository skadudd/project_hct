  fin_table AS(
    SELECT 
      Event_Date, Airbridge_Device_ID,Airbridge_Device_ID_Type,
      min(UUID) AS User_ID,  min(Platform) AS Platform,
      # -- 기타 정보 구조체 생성 -- #
      STRUCT(
        min(Device_Model) as Device_Model ,min(Device_Type) as Device_Type ,min(Platform) as Platform, 
        min(Client_IP_Country_Code) as Client_IP_Country_Code ,min(Client_IP_Subdivision) as Client_IP_Subdivision, 
        min(Client_IP_City) as Client_IP_City ,min(Channel) as Channel ,min(Campaign_ID) as Campaign_ID ,min(Ad_Group_ID) as Ad_Group_ID, 
        min(Ad_Creative_ID) as Ad_Creative_ID ,min(Term_ID) as Term_ID, min(Is_Re_engagement) as Is_Re_engagement, 
        min(Is_First_Event_per_User_ID) as Is_First_Event_per_User_ID, min(Is_First_Event_per_Device_ID) as Is_First_Event_per_Device_ID
        ,min(Is_First_Target_Event_per_Device) as Is_First_Target_Event_per_Device
        ,min(Target_Event_Timestamp) as Target_Event_Timestamp, min(Target_Event_Category) as Target_Event_Category
      ) AS i,
      Event_Category,SUM(Event_Value_Sum) as Event_Value_Total, SUM(Event_Count) as Event_Count_Total,
      # -- 이벤트 상세 구조체 생성. 2중 구조체 중 outter에 해당 -- #
      ARRAY_AGG(
        STRUCT(
          Event_Label as Label, Event_Action as Action, Event_Value_Sum as Event_Value_Sum, Event_Count, u as u, d as d
        )
      ) as e
    FROM(
      # -- 서브쿼리 시작 -- #
      SELECT 
          Event_Date, Airbridge_Device_ID, Airbridge_Device_ID_Type,
          # -- 유저 ID 구조체 생성 -- #
          ARRAY_AGG(
            STRUCT(
              User_ID as User_ID
            )
          ) as u,
          
          MIN(User_ID) as UUID, Event_Category, Event_Label, Event_Action, SUM(Event_Value) AS Event_Value_Sum, COUNT(*) AS Event_Count,
          min(Device_Model) as Device_Model ,min(Device_Type) as Device_Type ,min(Platform) as Platform ,min(Client_IP_Country_Code) as Client_IP_Country_Code ,min(Client_IP_Subdivision) as Client_IP_Subdivision ,min(Client_IP_City) as Client_IP_City
          ,min(Channel) as Channel, min(Campaign_ID) as Campaign_ID ,min(Ad_Group_ID) as Ad_Group_ID ,min(Ad_Creative_ID) as Ad_Creative_ID ,min(Term_ID) as Term_ID
          ,min(Is_Re_engagement) as Is_Re_engagement, min(Is_First_Event_per_User_ID) as Is_First_Event_per_User_ID, min(Is_First_Event_per_Device_ID) as Is_First_Event_per_Device_ID
          ,min(Is_First_Target_Event_per_Device) as Is_First_Target_Event_per_Device
          ,min(Target_Event_Timestamp) as Target_Event_Timestamp, min(Target_Event_Category) as Target_Event_Category,

            # -- 이벤트 상세 구조체 생성. 2중 구조체 중 inner에 해당 -- #
            ARRAY_AGG(STRUCT(
              Event_Datetime as Timestamp ,Event_Label as Label, Event_Action as Action, Event_Value as Value, commission, param, challenge_id, campaignType, 
              campaignName, gettableCash, quantity, score, position, scheduleID, type, achievementID, rate, price, tester, incentive_product_name, list_order, 
              contributionMargin, sharedChannel, listID, transactionID, brandID, transactionPairedEventCategory, originalContributionMargin, productListID, 
              datetime, place, name, totalQuantity, productID, transactionType, 
              query, isRenewal,
              products_str
            )) AS d
      FROM order_table
      GROUP BY Event_Date, Airbridge_Device_ID, Airbridge_Device_ID_Type, Event_Category, Event_Label, Event_Action
    ) AS inn
    GROUP BY Event_Date, Airbridge_Device_ID, Airbridge_Device_ID_Type, Event_Category
  )