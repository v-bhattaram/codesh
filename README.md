# Redshift connection setup (create engine only once outside the function)
redshift_config = {
    "host_url": "redshift-cluster.xxxxxx.us-west-2.redshift.amazonaws.com:5439/mydb",
    "user": "my_user",
    "password": "my_password",
    "aws_iam_role": "arn:aws:iam::123456789012:role/myRedshiftRole",  # Not used in this version
    "tempdir": "s3://my-bucket/temp/"  # Not used in this version
}

# Create SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{redshift_config['user']}:{redshift_config['password']}@{redshift_config['host_url']}"
)

# S3 path to CSV and target table in Redshift
s3_path = "s3://my-bucket/data/data.csv"
target_table = "public.my_table"

# Call the function with the connection object
load_csv_to_redshift_emr(s3_path, target_table, engine)


import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import boto3

def load_csv_to_redshift_emr(s3_path, target_table, conn):
    """
    Load a CSV file from S3 into a Redshift table.

    Args:
        s3_path (str): S3 path to CSV file (e.g., 's3://bucket/data/data.csv')
        target_table (str): Fully qualified Redshift table name (e.g., 'schema.table')
        conn (SQLAlchemy connection object): Active Redshift connection
    """
    # Initialize S3 client using boto3 (since we're on EMR)
    s3 = boto3.client('s3')

    # Parse the S3 path to get bucket and file key
    bucket_name = s3_path.split('/')[2]
    file_key = '/'.join(s3_path.split('/')[3:])

    # Read the CSV file from S3 using pandas (without needing to download to local)
    print(f"📥 Reading CSV from S3: s3://{bucket_name}/{file_key}")
    df = pd.read_csv(f"s3://{bucket_name}/{file_key}")

    # Clear the target table
    print("🧹 Clearing target table...")
    with conn.begin():
        conn.execute(f"DELETE FROM {target_table}")

    # Load the data into Redshift
    print("🚀 Loading data into Redshift...")
    df.to_sql(name=target_table.split('.')[-1],
              schema=target_table.split('.')[0],
              con=conn,
              if_exists='append',  # Use 'replace' to truncate before inserting
              index=False)

    print("✅ Data loaded successfully.")
