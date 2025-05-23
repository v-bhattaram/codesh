```
import csv
import boto3
from io import StringIO

def full_refresh_redshift_insert(conn, schema_name, table_name, s3_bucket, s3_key_path):
    """
    Perform a full refresh of Redshift table (truncate and insert row-by-row from S3 CSV).

    Parameters:
    - conn: psycopg2 connection object
    - schema_name: target Redshift schema
    - table_name: target Redshift table
    - s3_bucket: name of the S3 bucket
    - s3_key_path: full path to the CSV file in S3
    """

    # Step 1: Download CSV from S3 into memory
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_key_path)
    csv_data = obj['Body'].read().decode('utf-8')
    reader = csv.reader(StringIO(csv_data))

    # Step 2: Extract headers and rows
    headers = next(reader)
    rows = list(reader)

    # Step 3: Truncate target table
    cursor = conn.cursor()
    try:
        truncate_sql = f"TRUNCATE TABLE {schema_name}.{table_name};"
        cursor.execute(truncate_sql)
        print(f"Truncated {schema_name}.{table_name}")

        # Step 4: Insert rows manually
        placeholders = ','.join(['%s'] * len(headers))
        insert_sql = f"""
        INSERT INTO {schema_name}.{table_name} ({', '.join(headers)})
        VALUES ({placeholders});
        """

        for row in rows:
            cursor.execute(insert_sql, row)

        conn.commit()
        print(f"Inserted {len(rows)} rows into {schema_name}.{table_name}")

    except Exception as e:
        conn.rollback()
        print("Error during insert:", e)
        raise

    finally:
        cursor.close()
```
