```
WITH recursive cte AS (
  SELECT 
    id,
    delimited_column,
    1 AS position,
    SPLIT_PART(delimited_column, ';', 1) AS value
  FROM 
    your_table
  
  UNION ALL
  
  SELECT 
    id,
    delimited_column,
    position + 1,
    SPLIT_PART(delimited_column, ';', position + 1) AS value
  FROM 
    cte
  WHERE 
    position < LENGTH(delimited_column) - LENGTH(REPLACE(delimited_column, ';', '')) + 1
    AND SPLIT_PART(delimited_column, ';', position + 1) != ''
)

SELECT 
  id,
  value AS split_value
FROM 
  cte
WHERE 
  value != '';
```
