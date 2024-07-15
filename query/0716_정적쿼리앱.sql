# UDF 선언, JSON 파싱
CREATE TEMP FUNCTION extract_key_value(json STRING, key STRING)
RETURNS STRING
LANGUAGE js AS '''
  var obj = JSON.parse(json);
  return obj[key] || null;
''';

# UDF 선언, 키값 유무 조회
CREATE TEMP FUNCTION has_key(json STRING, key STRING)
RETURNS BOOL
LANGUAGE js AS '''
  var obj = JSON.parse(json);
  return obj.hasOwnProperty(key);
''';

# BEGIN ~ END
BEGIN

  # ---------------------------------------------------------------- #
  # 변수명 선언 사용되는 횟수와 같아야 함.
  # ---------------------------------------------------------------- #   
  DECLARE key_list STRING DEFAULT '';
  DECLARE struct_fields STRING DEFAULT '';

  # ---------------------------------------------------------------- #
  # !!!!!!!!!!! 다이나믹 피봇 쿼리를 위한 키 리스트 생성
  # ---------------------------------------------------------------- #   
  SET key_list = (
    WITH json_table AS (
        SELECT 
            Event_Date, Event_Datetime, 
            Airbridge_Device_ID, Airbridge_Device_ID_Type, User_ID,
            Channel, Campaign_ID, Ad_Group_ID, Ad_Creative_ID, Term_ID,
            Device_Model, Device_Type, Platform, Client_IP_Country_Code, Client_IP_Subdivision, Client_IP_City, 
            Is_Re_engagement, Is_First_Event_per_User_ID, Is_First_Event_per_Device_ID,
            Is_First_Target_Event_per_Device, Target_Event_Timestamp, Target_Event_Category,
            Event_Category, Event_Label, Event_Action, Event_Value,
            CASE 
                WHEN Custom_Event_Properties = '{}' THEN '{"tester": "true"}'
                ELSE Custom_Event_Properties
            END AS Custom_Event_Properties,
            Semantic_Event_Properties
        FROM 
            airbridge_lake.app_2024
        WHERE
            Event_Date =  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ),

    keys_extracted AS (
      SELECT
        REGEXP_EXTRACT_ALL(Semantic_Event_Properties, r'"([^"]+)":') AS semantic_keys,
        REGEXP_EXTRACT_ALL(Custom_Event_Properties, r'"([^"]+)":') AS custom_keys
      FROM
        json_table
    ),

    distinct_keys AS (
      SELECT DISTINCT key
      FROM keys_extracted,
      UNNEST(semantic_keys) AS key
      WHERE key NOT IN ('products') -- Nested 구조를 띌 가능성 있는 Key 값 추가
      UNION DISTINCT
      SELECT DISTINCT key
      FROM keys_extracted,
      UNNEST(custom_keys) AS key
    )

    SELECT
      CONCAT('("', STRING_AGG(key, '", "'), '")') AS keys
    FROM distinct_keys
  );

  -- Set the dynamic struct fields for the final SELECT
  SET struct_fields = (
    WITH json_table AS (
        SELECT 
            Event_Date, Event_Datetime,
            Airbridge_Device_ID, Airbridge_Device_ID_Type, User_ID,
            Channel, Campaign_ID, Ad_Group_ID, Ad_Creative_ID, Term_ID,
            Device_Model, Device_Type, Platform, Client_IP_Country_Code, Client_IP_Subdivision, Client_IP_City, 
            Is_Re_engagement, Is_First_Event_per_User_ID, Is_First_Event_per_Device_ID,
            Is_First_Target_Event_per_Device, Target_Event_Timestamp, Target_Event_Category,
            Event_Category, Event_Label, Event_Action, Event_Value,
            CASE 
                WHEN Custom_Event_Properties = '{}' THEN '{"tester": "true"}'
                ELSE Custom_Event_Properties
            END AS Custom_Event_Properties,
            Semantic_Event_Properties
        FROM 
            airbridge_lake.app_2024
        WHERE
            Event_Date =  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ),

    keys_extracted AS (
      SELECT
        REGEXP_EXTRACT_ALL(Semantic_Event_Properties, r'"([^"]+)":') AS semantic_keys,
        REGEXP_EXTRACT_ALL(Custom_Event_Properties, r'"([^"]+)":') AS custom_keys
      FROM
        json_table
    ),

    distinct_keys AS (
      SELECT DISTINCT key
      FROM keys_extracted,
      UNNEST(semantic_keys) AS key
      WHERE key NOT IN ('products') -- Nested 구조를 띌 가능성 있는 Key 값 추가
      UNION DISTINCT
      SELECT DISTINCT key
      FROM keys_extracted,
      UNNEST(custom_keys) AS key
    )

    SELECT
      STRING_AGG(CONCAT('IFNULL(', key, ', NULL) AS ', key), ', ') AS keys
    FROM distinct_keys
  );

  -- Check if key_list or struct_fields are NULL
  IF key_list IS NULL THEN
    SET key_list = '("dummy_key")';
  END IF;
  
  IF struct_fields IS NULL THEN
    SET struct_fields = 'IFNULL(dummy_key, NULL) AS dummy_key';
  END IF;

  # ---------------------------------------------------------------- #
  # !!!!!!!!!!! 다이나믹 피봇 쿼리 실행 Part
  # ---------------------------------------------------------------- #   
  EXECUTE IMMEDIATE FORMAT('''

  INSERT INTO `ballosodeuk.airbridge_mart.app_keyfeatures` 

  WITH json_table AS (
      SELECT 
        Event_Date, Event_Datetime, 
        Airbridge_Device_ID, Airbridge_Device_ID_Type, User_ID,
        Channel, Campaign_ID, Ad_Group_ID, Ad_Creative_ID, Term_ID,
        Device_Model, Device_Type, Platform, Client_IP_Country_Code, Client_IP_Subdivision, Client_IP_City, 
        Is_Re_engagement, Is_First_Event_per_User_ID, Is_First_Event_per_Device_ID,
        Is_First_Target_Event_per_Device, Target_Event_Timestamp, Target_Event_Category,
        Event_Category, Event_Label, Event_Action, Event_Value,
        CASE 
            WHEN Custom_Event_Properties = '{}' THEN '{"tester": "true"}'
            ELSE Custom_Event_Properties
        END AS Custom_Event_Properties,
        Semantic_Event_Properties
    FROM airbridge_lake.app_2024
    WHERE Event_Date =  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  ),

  # ---------------------------------------------------------------- #
  # JSON 내 키값 추출 및 중복 처리
  # ---------------------------------------------------------------- #    

  keys_extracted AS (
    SELECT
      *,
      REGEXP_EXTRACT_ALL(Semantic_Event_Properties, r'"([^"]+)":') AS semantic_keys,
      REGEXP_EXTRACT_ALL(Custom_Event_Properties, r'"([^"]+)":') AS custom_keys
    FROM
      json_table
  ),

  filtered_keys AS (
    SELECT
      *,
      ARRAY(
        SELECT key FROM UNNEST(semantic_keys) AS key
        WHERE key NOT IN ('products')
        UNION DISTINCT
        SELECT key FROM UNNEST(custom_keys) AS key
      ) AS filtered_keys
    FROM
      keys_extracted
  ),

  # ---------------------------------------------------------------- #
  # Key 테이블 생성
  # 1. filtered_key 배열 사용하여 key열 생성
  # 2. UDF 사용하여 JSON 파싱
  # ---------------------------------------------------------------- #    

  unnested_semantic_keys AS (
    SELECT
        Event_Date, Event_Datetime, 
        Airbridge_Device_ID, Airbridge_Device_ID_Type, User_ID,
        Channel, Campaign_ID, Ad_Group_ID, Ad_Creative_ID, Term_ID,
        Device_Model, Device_Type, Platform, Client_IP_Country_Code, Client_IP_Subdivision, Client_IP_City, 
        Is_Re_engagement, Is_First_Event_per_User_ID, Is_First_Event_per_Device_ID,
        Is_First_Target_Event_per_Device, Target_Event_Timestamp, Target_Event_Category,
        Event_Category, Event_Label, Event_Action, Event_Value,
        Semantic_Event_Properties, Custom_Event_Properties,
        key,
        extract_key_value(Semantic_Event_Properties, key) AS value
    FROM
      filtered_keys,
      UNNEST(filtered_keys.filtered_keys) AS key
  ),

  unnested_custom_keys AS (
    SELECT
        Event_Date, Event_Datetime,
        Airbridge_Device_ID, Airbridge_Device_ID_Type, User_ID,
        Channel, Campaign_ID, Ad_Group_ID, Ad_Creative_ID, Term_ID,
        Device_Model, Device_Type, Platform, Client_IP_Country_Code, Client_IP_Subdivision, Client_IP_City, 
        Is_Re_engagement, Is_First_Event_per_User_ID, Is_First_Event_per_Device_ID,
        Is_First_Target_Event_per_Device, Target_Event_Timestamp, Target_Event_Category,
        Event_Category, Event_Label, Event_Action, Event_Value,
        Semantic_Event_Properties, Custom_Event_Properties,
        key,
        extract_key_value(Custom_Event_Properties, key) AS value
    FROM
      filtered_keys,
      UNNEST(filtered_keys.filtered_keys) AS key
  ),

  # ---------------------------------------------------------------- #
  # Key 테이블 CONCAT
  # ---------------------------------------------------------------- #  

  combined_keys AS (
    SELECT
        Event_Date, Event_Datetime, 
        Airbridge_Device_ID, Airbridge_Device_ID_Type, User_ID,
        Channel, Campaign_ID, Ad_Group_ID, Ad_Creative_ID, Term_ID,
        Device_Model, Device_Type, Platform, Client_IP_Country_Code, Client_IP_Subdivision, Client_IP_City, 
        Is_Re_engagement, Is_First_Event_per_User_ID, Is_First_Event_per_Device_ID,
        Is_First_Target_Event_per_Device, Target_Event_Timestamp, Target_Event_Category,
        Event_Category, Event_Label, Event_Action, Event_Value,
        Semantic_Event_Properties, Custom_Event_Properties,
        key,
        extract_key_value(Semantic_Event_Properties, key) AS value
    FROM
        unnested_semantic_keys
    UNION ALL
    SELECT
        Event_Date, Event_Datetime, 
        Airbridge_Device_ID, Airbridge_Device_ID_Type, User_ID,
        Channel, Campaign_ID, Ad_Group_ID, Ad_Creative_ID, Term_ID,
        Device_Model, Device_Type, Platform, Client_IP_Country_Code, Client_IP_Subdivision, Client_IP_City, 
        Is_Re_engagement, Is_First_Event_per_User_ID, Is_First_Event_per_Device_ID,
        Is_First_Target_Event_per_Device, Target_Event_Timestamp, Target_Event_Category,
        Event_Category, Event_Label, Event_Action, Event_Value,
        Semantic_Event_Properties, Custom_Event_Properties,
        key,
        extract_key_value(Custom_Event_Properties, key) AS value
    FROM
        unnested_custom_keys
  ),

  # ---------------------------------------------------------------- #
  # Products Nested JSON 테이블 생성
  # 1. 2중 JSON, Null 값 등, 이슈 발생하는 요인이므로, 따로 테이블 처리 (branch)
  # 2. 생성 후 머지 예정
  # ---------------------------------------------------------------- #  

  product_keys_extracted AS (
    SELECT
      Event_Date, Event_Datetime, Airbridge_Device_ID, Event_Category, Event_Label, Event_Action, Event_Value,
      IFNULL(JSON_EXTRACT_ARRAY(Semantic_Event_Properties, '$.products'), []) AS products
    FROM
      json_table
  ),

  products_unnested AS (
    SELECT
      Event_Date, Event_Datetime, Airbridge_Device_ID, Event_Category, Event_Label, Event_Action, Event_Value,
      JSON_EXTRACT_SCALAR(product, '$.name') AS name,
      JSON_EXTRACT_SCALAR(product, '$.price') AS price,
      JSON_EXTRACT_SCALAR(product, '$.position') AS position,
      JSON_EXTRACT_SCALAR(product, '$.quantity') AS quantity,
      JSON_EXTRACT_SCALAR(product, '$.productID') AS productID,
      JSON_EXTRACT_SCALAR(product, '$.brandID') AS brandID,
      JSON_EXTRACT_SCALAR(product, '$.currency') AS currency
    FROM
      product_keys_extracted,
      UNNEST(products) AS product
  ),

  products_struct AS (
    SELECT
      Event_Date, Event_Datetime, Airbridge_Device_ID, Event_Category, Event_Label, Event_Action, Event_Value,
      ARRAY_AGG(STRUCT(name as name, price as price, position as position, quantity as quantity, 
        productID as productID, brandID as brandID, currency as currency )) AS products_struct
    FROM products_unnested
    GROUP BY Event_Date, Event_Datetime, Airbridge_Device_ID, Event_Category, Event_Label, Event_Action, Event_Value
  ),

  # ---------------------------------------------------------------- #
  # 피봇 테이블 생성
  # 1. Semantic, Custom JSON 파싱하여 key, value 열을 포함한 테이블 select
  # 2. 선언한 변수 사용하여 key-value 피봇.
  # ---------------------------------------------------------------- #  

  pivot_table AS (
    SELECT *
    FROM combined_keys
    PIVOT (
      ANY_VALUE(value) FOR key IN (
        "Timestamp", "Label", "Action", "Value", "commission", "param", "challenge_id", "campaignType", "campaignName", "gettableCash", "quantity", 
        "score", "position", "scheduleID", "type", "achievementID", "rate", "price", "tester", "incentive_product_name", "list_order", 
        "contributionMargin", "sharedChannel",       
        "listID", "transactionID", "brandID", "transactionPairedEventCategory", "originalContributionMargin", "productListID", "datetime", "place", "name", 
        "totalQuantity", "productID", "transactionType","query", "isRenewal")
    )
  ),

  # ---------------------------------------------------------------- #
  # 머지 테이블 생성
  # 1. Semantic, Custom JSON 파싱 및 피봇한 Pivot 테이블 Select
  # 2. Semantic 내 Product Nested JSON 파싱 및 구조화한 테이블 Select
  # 3. Key 값 기준 머지
  # ---------------------------------------------------------------- #

  merged_table AS (
    SELECT
        table_1.Event_Date, table_1.Event_Datetime, 
        table_1.Airbridge_Device_ID, table_1.Airbridge_Device_ID_Type, table_1.User_ID,
        table_1.Channel, table_1.Campaign_ID, table_1.Ad_Group_ID, table_1.Ad_Creative_ID, table_1.Term_ID,
        table_1.Device_Model, table_1.Device_Type, table_1.Platform, table_1.Client_IP_Country_Code, table_1.Client_IP_Subdivision, table_1.Client_IP_City, 
        table_1.Is_Re_engagement, table_1.Is_First_Event_per_User_ID, table_1.Is_First_Event_per_Device_ID,
        table_1.Is_First_Target_Event_per_Device, table_1.Target_Event_Timestamp, table_1.Target_Event_Category,
        table_1.Event_Category, table_1.Event_Label, table_1.Event_Action, table_1.Event_Value,
        table_1.Semantic_Event_Properties, table_1.Custom_Event_Properties,
        table_2.products_struct AS products_str,
        table_1.* EXCEPT (
          Event_Date, Event_Datetime, 
          Airbridge_Device_ID, Airbridge_Device_ID_Type, User_ID,
          Channel, Campaign_ID, Ad_Group_ID, Ad_Creative_ID, Term_ID,
          Device_Model, Device_Type, Platform, Client_IP_Country_Code, Client_IP_Subdivision, Client_IP_City, 
          Is_Re_engagement, Is_First_Event_per_User_ID, Is_First_Event_per_Device_ID,
          Is_First_Target_Event_per_Device, Target_Event_Timestamp, Target_Event_Category,
          Event_Category, Event_Label, Event_Action, Event_Value, Semantic_Event_Properties, Custom_Event_Properties
        ),
        table_2.* EXCEPT (
          Event_Date, Event_Datetime, Airbridge_Device_ID, Event_Category, Event_Label, Event_Action, Event_Value
        )
    FROM
      pivot_table as table_1
    LEFT JOIN 
      products_struct as table_2
      ON table_1.Event_Date = table_2.Event_Date AND 
       table_1.Event_Datetime = table_2.Event_Datetime AND 
       table_1.Airbridge_Device_ID = table_2.Airbridge_Device_ID AND
       table_1.Event_Category = table_2.Event_Category AND
       table_1.Event_Label = table_2.Event_Label AND
       table_1.Event_Action = table_2.Event_Action AND
       table_1.Event_Value = table_2.Event_Value
  ),

  # ---------------------------------------------------------------- #
  # 머지 테이블 순서 정렬 (주요 기능 없음)
  # ---------------------------------------------------------------- #

  order_table AS (
    SELECT 
      Event_Date, Airbridge_Device_ID, Airbridge_Device_ID_Type, 
      Device_Model, Device_Type, Platform, Client_IP_Country_Code, Client_IP_Subdivision, Client_IP_City, 
      User_ID, 
      Channel, Campaign_ID, Ad_Group_ID, Ad_Creative_ID, Term_ID,
      Is_Re_engagement, Is_First_Event_per_User_ID, Is_First_Event_per_Device_ID,
      Is_First_Target_Event_per_Device, Target_Event_Timestamp, Target_Event_Category,
      Event_Datetime, Event_Category, Event_Label, Event_Action, Event_Value,
      merged_table.* EXCEPT (
        Event_Date, Airbridge_Device_ID, Airbridge_Device_ID_Type, 
        Device_Model, Device_Type, Platform, Client_IP_Country_Code, Client_IP_Subdivision, Client_IP_City, 
        User_ID, Channel, Campaign_ID, Ad_Group_ID, Ad_Creative_ID, Term_ID,
        Is_Re_engagement, Is_First_Event_per_User_ID, Is_First_Event_per_Device_ID,
        Is_First_Target_Event_per_Device, Target_Event_Timestamp, Target_Event_Category,
        Event_Datetime, Event_Category, Event_Label, Event_Action, Event_Value
      )
    FROM merged_table
  ),

  # ---------------------------------------------------------------- #
  # 최종 테이블
  # 1. semantic, custom 컬럼을 2중 구조체로 생성
  # 2. Event Action, Event_Label을 Date, Timestamp 기준 2중 Group by
  # 3. 2중 Group by 된 각 이벤트의 Value, Count 피쳐 생성
  # 4. 1:N 구조의 User_ID, 구조체 생성
  # 5. 디바이스 정보, 캠페인 정보 등, 구조체 생성
  # ---------------------------------------------------------------- #

  fin_table AS(
    SELECT 
      Event_Date, Airbridge_Device_ID,Airbridge_Device_ID_Type,
      min(UUID) AS User_ID,  min(Platform) AS Platform,
      # -- 기타 정보 구조체 생성 -- #
      STRUCT(
        min(Device_Model) as Device_Model ,min(Device_Type) as Device_Type ,min(Platform) as Platform ,min(Client_IP_Country_Code) as Client_IP_Country_Code ,min(Client_IP_Subdivision) as Client_IP_Subdivision ,min(Client_IP_City) as Client_IP_City
        ,min(Channel) as Channel ,min(Campaign_ID) as Campaign_ID ,min(Ad_Group_ID) as Ad_Group_ID ,min(Ad_Creative_ID) as Ad_Creative_ID ,min(Term_ID) as Term_ID
        ,min(Is_Re_engagement) as Is_Re_engagement, min(Is_First_Event_per_User_ID) as Is_First_Event_per_User_ID, min(Is_First_Event_per_Device_ID) as Is_First_Event_per_Device_ID
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
            products_str,
            query, isRenewal
          )) AS d
      FROM order_table
      GROUP BY Event_Date, Airbridge_Device_ID, Airbridge_Device_ID_Type, Event_Category, Event_Label, Event_Action
    ) AS inn
    GROUP BY Event_Date, Airbridge_Device_ID, Airbridge_Device_ID_Type, Event_Category
  )
  
  select * from fin_table
  
  
  ''');
END;
