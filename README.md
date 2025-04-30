def update_srcquery_from_s3(bucket_name, prefix, sqlite_db_path, table_name):
    s3 = boto3.client('s3')
    
    # List objects in the specified S3 folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' not in response:
        print("No files found in the specified prefix.")
        return
    
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    
    for obj in response['Contents']:
        key = obj['Key']
        
        # Skip folders
        if key.endswith('/'):
            continue
        
        # Get file content
        s3_object = s3.get_object(Bucket=bucket_name, Key=key)
        file_contents = s3_object['Body'].read().decode('utf-8')
        
        # Extract filename only
        filename = key.split('/')[-1]
        
        # Update the database table
        cursor.execute(f"""
            UPDATE {table_name}
            SET srcquery = ?
            WHERE script = ?
        """, (file_contents, filename))
        
        print(f"Updated '{filename}' in table '{table_name}'.")
    
    conn.commit()
    conn.close()
