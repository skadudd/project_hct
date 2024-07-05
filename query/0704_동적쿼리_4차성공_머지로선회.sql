-- Define the UDF to extract key values from JSON
CREATE TEMP FUNCTION extract_key_value(json STRING, key STRING)
RETURNS STRING
LANGUAGE js AS """
  var obj = JSON.parse(json);
  return obj[key];
""";

-- Use a BEGIN...END block to declare variables and execute dynamic SQL
BEGIN
  -- Declare the variable to hold the dynamic key list for pivot
  DECLARE key_list STRING;

  -- Set the dynamic key list for pivot
  SET key_list = (
    WITH json_table AS (
        SELECT 
            Event_Date, Event_Datetime, Cookie_ID,
            Event_Category, Event_Label, Event_Action, Event_Value,
            Semantic_Event_Properties, Custom_Event_Properties
        FROM 
            `airbridge_lake.temp_table`
        WHERE
            Event_Date = '2024-07-02'
    ),

    keys_extracted AS (
      SELECT
        REGEXP_EXTRACT_ALL(Semantic_Event_Properties, r'"([^"]+)":') AS semantic_keys,
        REGEXP_EXTRACT_ALL(Custom_Event_Properties, r'"([^"]+)":') AS custom_keys,
      FROM
        json_table
    ),

    distinct_keys AS (
      SELECT DISTINCT key
      FROM keys_extracted,
      UNNEST(semantic_keys) AS key
      WHERE key NOT IN ('products') #Nested 구조를 띌 가능성 있는 Key 값 추가
      UNION DISTINCT
      SELECT DISTINCT key
      FROM keys_extracted,
      UNNEST (custom_keys) AS key
    )

    SELECT
      CONCAT('("', STRING_AGG(key, '", "'), '")') AS keys
    FROM distinct_keys
  );

  -- Execute the dynamic pivot query
  EXECUTE IMMEDIATE FORMAT("""
  WITH json_table AS (
      SELECT 
          Event_Date, Event_Datetime, Cookie_ID,
          Event_Category, Event_Label, Event_Action, Event_Value,
          Semantic_Event_Properties, Custom_Event_Properties
      FROM 
          `airbridge_lake.temp_table`
      WHERE
          Event_Date = '2024-07-02'
  ),

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

  unnested_semantic_keys AS (
    SELECT
      Event_Date, Event_Datetime, Cookie_ID,
      Event_Category, Event_Label, Event_Action, Event_Value, Semantic_Event_Properties,
      key,
      extract_key_value(Semantic_Event_Properties, key) AS value
    FROM
      filtered_keys,
      UNNEST(filtered_keys.filtered_keys) AS key
  ),
  
  unnested_custom_keys AS (
    SELECT
      Event_Date, Event_Datetime, Cookie_ID,
      Event_Category, Event_Label, Event_Action, Event_Value, Semantic_Event_Properties,
      key,
      extract_key_value(Custom_Event_Properties, key) AS value
    FROM
      filtered_keys,
      UNNEST(filtered_keys.filtered_keys) AS key
  ),

  combined_keys AS (
    SELECT
        Event_Date, Event_Datetime, Cookie_ID,
        Event_Category, Event_Label, Event_Action, Event_Value, Semantic_Event_Properties,
        key,
        value
    FROM
        unnested_semantic_keys
    UNION ALL
    SELECT
        Event_Date, Event_Datetime, Cookie_ID,
        Event_Category, Event_Label, Event_Action, Event_Value, Semantic_Event_Properties, 
        key,
        value
    FROM
        unnested_custom_keys
  ),

  product_keys_extracted AS (
    SELECT
      Event_Date, Event_Datetime, Cookie_ID,
      Event_Category, Event_Label, Event_Action, Event_Value, Semantic_Event_Properties,
      JSON_EXTRACT_ARRAY(Semantic_Event_Properties, '$.products') AS products
    FROM
      json_table
  ),

  products_unnested AS (
    SELECT
      Event_Date, Event_Datetime, Cookie_ID,
      Event_Category, Event_Label, Event_Action, Event_Value, Semantic_Event_Properties, 
      JSON_EXTRACT_SCALAR(product, '$.name') AS name,
      JSON_EXTRACT_SCALAR(product, '$.price') AS price,
      JSON_EXTRACT_SCALAR(product, '$.position') AS position
    FROM
      product_keys_extracted,
      UNNEST(products) AS product
  ),

  products_struct AS (
    SELECT
      Event_Date, Event_Datetime, Cookie_ID,
      Event_Category, Event_Label, Event_Action, Event_Value,
      ARRAY_AGG(STRUCT(name as name, price as price, position as position)) AS products,
      min(Semantic_Event_Properties)
    FROM products_unnested
    GROUP BY Event_Date, Event_Datetime, Cookie_ID, Event_Category, Event_Label, Event_Action, Event_Value
  ),
  
  pivot_table AS (
    SELECT *
    FROM combined_keys
    PIVOT (
      ANY_VALUE(value) FOR key IN %s
    )
  ),

  merged_table AS (
    SELECT
      *
    FROM
      pivot_table as table_1
    LEFT JOIN 
      products_struct as table_2
      ON table_1.Event_Date = table_2.Event_Date AND 
       table_1.Event_Datetime = table_2.Event_Datetime AND 
       table_1.Cookie_ID = table_2.Cookie_ID AND
       table_1.Event_Category = table_2.Event_Category AND
       table_1.Event_Label = table_2.Event_Label AND
       table_1.Event_Action = table_2.Event_Action AND
       table_1.Event_Value = table_2.Event_Value
    
  )

  select *
  from merged_table


  """, key_list);
END;


