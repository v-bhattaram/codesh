```python
import pyodbc

def generate_merge_statement(conn_str, source_schema, source_table, target_schema, target_table):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Get column info
    query_columns = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query_columns, target_schema, target_table)
    columns = [row.COLUMN_NAME for row in cursor.fetchall()]
    if not columns:
        return f"-- Target table {target_schema}.{target_table} not found."

    # Try to get a unique constraint or primary key
    query_unique_cols = f"""
        SELECT k.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
          ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
        WHERE t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ?
          AND t.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
        ORDER BY k.ORDINAL_POSITION
    """
    cursor.execute(query_unique_cols, target_schema, target_table)
    key_columns = [row.COLUMN_NAME for row in cursor.fetchall()]
    if not key_columns:
        key_columns = columns  # fallback to all columns

    # Prepare merge components
    on_clause = " AND ".join(
        [f"T.[{col}] = S.[{col}]" for col in key_columns]
    )
    update_clause = ",\n        ".join(
        [f"T.[{col}] = S.[{col}]" for col in columns if col not in key_columns]
    )
    insert_columns = ", ".join([f"[{col}]" for col in columns])
    insert_values = ", ".join([f"S.[{col}]" for col in columns])

    # Assemble the full MERGE statement
    merge_sql = f"""MERGE [{target_schema}].[{target_table}] AS T
USING [{source_schema}].[{source_table}] AS S
ON {on_clause}
WHEN MATCHED THEN
    UPDATE SET
        {update_clause}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insert_columns})
    VALUES ({insert_values});
"""
    cursor.close()
    conn.close()
    return merge_sql
```
