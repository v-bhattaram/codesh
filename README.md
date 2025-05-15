```
from IPython.core.display import display, HTML
import pandas as pd

def show_merge_preview_html(conn, source_schema, source_table, target_schema, target_table, key_columns):
    """
    Fetches data from source and target Redshift tables and displays an HTML table comparing them.

    Args:
        conn: DB connection object (psycopg2 or redshift_connector).
        source_schema (str): Schema of the source table.
        source_table (str): Source table name.
        target_schema (str): Schema of the target table.
        target_table (str): Target table name.
        key_columns (list): List of column names used as the primary key for comparison.
    """
    def fetch_table(schema, table, columns=None):
        col_str = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        query = f'SELECT {col_str} FROM "{schema}"."{table}"'
        return pd.read_sql(query, conn)

    # Get source columns
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s 
            ORDER BY ordinal_position
        """, (source_schema, source_table))
        source_columns = [row[0] for row in cur.fetchall()]

    # Fetch both tables using only source columns
    source_df = fetch_table(source_schema, source_table, source_columns)
    target_df = fetch_table(target_schema, target_table, source_columns)

    # Set index for comparison
    source_df_indexed = source_df.set_index(key_columns)
    target_df_indexed = target_df.set_index(key_columns)

    html_rows = []

    for idx, source_row in source_df_indexed.iterrows():
        if idx not in target_df_indexed.index:
            row_type = 'insert'
        else:
            target_row = target_df_indexed.loc[idx]
            row_type = 'update' if not source_row.equals(target_row) else 'same'

        if row_type == 'insert':
            color = 'lightgreen'
        elif row_type == 'update':
            color = 'lightpink'
        else:
            color = 'white'

        # Ensure idx is a tuple for multi-column keys
        idx_values = idx if isinstance(idx, tuple) else (idx,)
        full_row = list(idx_values) + list(source_row.values)
        html_row = f"<tr style='background-color:{color}'>" + \
                   ''.join([f"<td>{val}</td>" for val in full_row]) + \
                   "</tr>"
        html_rows.append(html_row)

    # Combine key + non-key columns
    display_columns = key_columns + [col for col in source_columns if col not in key_columns]
    header_html = "<tr>" + ''.join([f"<th>{col}</th>" for col in display_columns]) + "</tr>"
    full_table = f"<table border='1' style='border-collapse:collapse'>{header_html}{''.join(html_rows)}</table>"

    display(HTML(full_table))

```
