```
CREATE TABLE run_control (
    dataset_name VARCHAR(100) PRIMARY KEY,
    run_id       UUID NOT NULL,
    created_ts   TIMESTAMP DEFAULT SYSDATE
);


BEGIN;

-- Step 1: Generate run_id once in-session
-- Use a CTE so the UUID is stable across all inserts
WITH new_run AS (
    SELECT uuid_generate_v4() AS run_id
)

-- Step 2: Create a fresh staging table
DROP TABLE IF EXISTS sales_data_staging;

CREATE TABLE sales_data_staging (LIKE sales_data_base);

-- Step 3: Load data into staging with the generated run_id
INSERT INTO sales_data_staging (run_id, order_id, amount)
SELECT (SELECT run_id FROM new_run),
       order_id,
       amount
FROM staging_sales;

-- Step 4: Drop old production table
DROP TABLE IF EXISTS sales_data;

-- Step 5: Rename staging → production
ALTER TABLE sales_data_staging RENAME TO sales_data;

-- Step 6: Update control table (insert or update latest run_id)
INSERT INTO run_control (dataset_name, run_id)
SELECT 'sales_data', run_id
FROM new_run
ON CONFLICT (dataset_name) DO UPDATE
SET run_id     = EXCLUDED.run_id,
    created_ts = SYSDATE;

COMMIT;

```
