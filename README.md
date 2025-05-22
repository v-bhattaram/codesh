```
import boto3
import csv
import io
import logging

def insert_csv_to_redshift_all_strings(
    conn,
    s3_bucket: str,
    s3_key: str,
    table_name: str,
    schema: str = "public",
    region: str = "us-east-1",
    delimiter: str = ",",
    has_header: bool = True
):
    """
    Download a CSV from S3 and insert into Redshift with all columns treated as strings.

    Args:
        conn: Redshift connection object (psycopg2 or redshift_connector)
        s3_bucket (str): Name of the S3 bucket
        s3_key (str): Key (path) of the CSV file
        table_name (str): Redshift table name
        schema (str): Redshift schema name
        region (str): AWS region
        delimiter (str): CSV field delimiter
        has_header (bool): Whether the first row is a header
    """
    s3 = boto3.client('s3', region_name=region)
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    content = response['Body'].read().decode('utf-8')
    reader = csv.reader(io.StringIO(content), delimiter=delimiter)

    rows = list(reader)
    if not rows:
        logging.warning("CSV is empty.")
        return

    if has_header:
        header = rows.pop(0)
    else:
        header = [f"col{i+1}" for i in range(len(rows[0]))]

    cursor = conn.cursor()
    table_full = f"{schema}.{table_name}"
    placeholders = ', '.join(['%s'] * len(header))
    insert_sql = f"INSERT INTO {table_full} ({', '.join(header)}) VALUES ({placeholders})"

    try:
        for row in rows:
            str_row = [str(val) if val is not None else None for val in row]
            cursor.execute(insert_sql, str_row)
        conn.commit()
        logging.info(f"Inserted {len(rows)} rows into {table_full}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to insert rows: {str(e)}")
        raise
    finally:
        cursor.close()

```
