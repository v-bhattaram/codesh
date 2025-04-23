```
import boto3
from pyspark.sql import SparkSession

# --- CONFIGURATION ---
bucket = "your-bucket-name"
prefix = "your/path/to/folder/"  # folder path in S3
region = "your-region"  # e.g., "us-east-1"

# --- 1. List CSV files in S3 folder ---
s3 = boto3.client("s3", region_name=region)

response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
csv_files = [
    f"s3://{bucket}/{obj['Key']}"
    for obj in response.get("Contents", [])
    if obj["Key"].endswith(".csv")
]

print(f"Found {len(csv_files)} CSV files in s3://{bucket}/{prefix}")
for f in csv_files:
    print(" -", f)

# --- 2. Read & summarize each CSV file ---
for file_path in csv_files:
    print("\n📄 File:", file_path)

    try:
        df = spark.read.option("header", "true").csv(file_path)

        print("🧱 Columns & Types:")
        for col in df.dtypes:
            print(f" - {col[0]}: {col[1]}")

        row_count = df.count()
        print(f"📊 Row Count: {row_count}")
        
    except Exception as e:
        print("❌ Error reading file:", e)

```
