```
WITH
-- CTE for MD5-based UUIDs
md5_data AS (
  SELECT 
    (a.n - 1) * 1000 + b.n AS row_num,
    MD5(CONCAT('md5_', a.n, '_', b.n, '_', FLOOR(RANDOM() * 100000)::INT)) AS md5_uuid
  FROM generate_series(1, 1000) a(n)
  CROSS JOIN generate_series(1, 1000) b(n)
),

-- CTE for FNV_HASH-based IDs
fnv_data AS (
  SELECT 
    (a.n - 1) * 1000 + b.n AS row_num,
    FNV_HASH(CONCAT('fnv_', a.n, '_', b.n, '_', FLOOR(RANDOM() * 100000)::INT)) AS fnv_hash
  FROM generate_series(1, 1000) a(n)
  CROSS JOIN generate_series(1, 1000) b(n)
)

-- Join on row_num
SELECT 
  m.row_num,
  m.md5_uuid,
  f.fnv_hash
FROM md5_data m
JOIN fnv_data f ON m.row_num = f.row_num
LIMIT 20; -- Remove limit to see all 1M rows
```
