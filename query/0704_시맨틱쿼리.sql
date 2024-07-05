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
            `airbridge_lake.web_2024`
        WHERE
            Event_Date = '2024-07-02'
    ),

    keys_extracted AS (
      SELECT
        REGEXP_EXTRACT_ALL(Semantic_Event_Properties, r'"([^"]+)":') AS semantic_keys
      FROM
        json_table
    ),

    distinct_keys AS (
      SELECT DISTINCT key
      FROM keys_extracted,
      UNNEST(semantic_keys) AS key
      WHERE key NOT IN ('products')
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
          `airbridge_lake.web_2024`
      WHERE
          Event_Date = '2024-07-02'
  ),

  keys_extracted AS (
    SELECT
      *,
      REGEXP_EXTRACT_ALL(Semantic_Event_Properties, r'"([^"]+)":') AS semantic_keys
    FROM
      json_table
  ),

  filtered_keys AS (
    SELECT
      *,
      ARRAY(
        SELECT key FROM UNNEST(semantic_keys) AS key
        WHERE key NOT IN ('products')
      ) AS filtered_keys
    FROM
      keys_extracted
  ),

  unnested_keys AS (
    SELECT
      Event_Date, Event_Datetime, Cookie_ID,
      Event_Category, Event_Label, Event_Action, Event_Value,
      key,
      extract_key_value(Semantic_Event_Properties, key) AS value
    FROM
      filtered_keys,
      UNNEST(filtered_keys.filtered_keys) AS key
  ),

  pivot_table AS (
    SELECT
      Event_Date, Event_Datetime, Cookie_ID,
      Event_Category, Event_Label, Event_Action, Event_Value,
      key, value
    FROM
      unnested_keys
  )

  SELECT *
  FROM pivot_table
  PIVOT (
    ANY_VALUE(value) FOR key IN %s
  );
  """, key_list);
END;
