```

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import psycopg2
from io import StringIO

# Create Spark session (assumes you're in EMR Notebook with Spark context)
spark = SparkSession.builder.getOrCreate()

def redshift_type_from_spark(datatype):
    """Map PySpark SQL types to Redshift types."""
    if isinstance(datatype, IntegerType):
        return "INTEGER"
    elif isinstance(datatype, LongType):
        return "BIGINT"
    elif isinstance(datatype, FloatType):
        return "REAL"
    elif isinstance(datatype, DoubleType):
        return "DOUBLE PRECISION"
    elif isinstance(datatype, BooleanType):
        return "BOOLEAN"
    elif isinstance(datatype, TimestampType):
        return "TIMESTAMP"
    elif isinstance(datatype, DateType):
        return "DATE"
    else:
        return "VARCHAR(256)"  # Default for StringType, BinaryType, etc.

def load_csv_to_redshift_emr(s3_path, target_table, conn):
    """
    Create a Redshift table and load a CSV file from S3 using PySpark and psycopg2.

    Args:
        s3_path (str): S3 path to the CSV (e.g., 's3://bucket/data.csv')
        target_table (str): Fully qualified Redshift table name (schema.table)
        conn (psycopg2 connection): Redshift connection
    """
    print(f"📥 Reading CSV from {s3_path} using PySpark...")
    df = spark.read.option("header", "true").csv(s3_path, inferSchema=True)

    # Extract schema and table name
    schema_name, table_name = target_table.split(".")

    # Generate CREATE TABLE SQL
    columns = []
    for field in df.schema.fields:
        redshift_type = redshift_type_from_spark(field.dataType)
        columns.append(f'"{field.name}" {redshift_type}')
    ddl = f"CREATE TABLE IF NOT EXISTS {target_table} (\n  {', '.join(columns)}\n);"

    # Execute DDL
    with conn.cursor() as cur:
        print(f"📐 Creating table {target_table}...")
        cur.execute(f"DROP TABLE IF EXISTS {target_table};")
        cur.execute(ddl)
        conn.commit()

    # Convert to pandas DataFrame for copy_expert
    print("🧪 Converting to pandas...")
    pandas_df = df.toPandas()

    # Prepare CSV for COPY
    buffer = StringIO()
    pandas_df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    columns_csv = ', '.join([f'"{col}"' for col in pandas_df.columns])

    print("🚀 Copying data to Redshift...")
    with conn.cursor() as cur:
        cur.copy_expert(f"""
            COPY {target_table} ({columns_csv})
            FROM STDIN
            DELIMITER ','
            CSV;
        """, buffer)
        conn.commit()

    print("✅ Table created and data loaded successfully.")


import psycopg2

# Connect to Redshift
conn = psycopg2.connect(
    host="your-redshift-cluster.amazonaws.com",
    port=5439,
    dbname="yourdb",
    user="youruser",
    password="yourpassword"
)

# Call the function
load_csv_to_redshift_emr("s3://your-bucket/data/data.csv", "public.new_table", conn)

conn.close()

```
