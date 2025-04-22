import boto3
from pyathena import connect
from pyathena.util import as_pandas
import os

# ---- Configurable Section ----
aws_access_key = "YOUR_ACCESS_KEY"
aws_secret_key = "YOUR_SECRET_KEY"
region_name = "us-east-1"  # Change as needed
s3_output = "s3://your-athena-query-results-bucket/folder/"  # Replace with your actual bucket/folder
database_name = "your_database_name"
view_file_path = "view.sql"
# --------------------------------

# Read SQL file
if not os.path.exists(view_file_path):
    raise FileNotFoundError(f"{view_file_path} not found.")

with open(view_file_path, "r") as f:
    view_sql = f.read().strip()

# Connect and run query
conn = connect(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name,
    s3_staging_dir=s3_output,
    schema_name=database_name
)

cursor = conn.cursor()
cursor.execute(view_sql)

print("✅ View created successfully.")
