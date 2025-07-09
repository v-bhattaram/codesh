```
import csv
import psycopg2
from psycopg2.extras import RealDictCursor

def get_column_details(conn, schema_name):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT table_name, ordinal_position, column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s
            ORDER BY table_name, ordinal_position
        """, (schema_name,))
        return cur.fetchall()

def get_constraints(conn, schema_name):
    pk_map = {}
    fk_map = {}

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Primary Keys
        cur.execute("""
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = %s
        """, (schema_name,))
        for row in cur.fetchall():
            pk_map.setdefault(row["table_name"], []).append(row["column_name"])

        # Foreign Keys
        cur.execute("""
            SELECT tc.table_name, kcu.column_name, ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %s
        """, (schema_name,))
        for row in cur.fetchall():
            fk_map.setdefault(row["table_name"], []).append({
                "column": row["column_name"],
                "foreign_table": row["foreign_table"],
                "foreign_column": row["foreign_column"]
            })

    return pk_map, fk_map

def get_table_keys(conn, schema_name):
    table_keys = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT tablename, 
                   distkey, 
                   sortkey1, 
                   "column" AS column_name
            FROM svv_table_info
            WHERE schema = %s
        """, (schema_name,))
        for row in cur.fetchall():
            table = row["tablename"]
            column = row["column_name"]
            table_keys.setdefault(table, {}).setdefault("distkey", "")
            table_keys.setdefault(table, {}).setdefault("sortkeys", [])
            if row["distkey"] == "t":
                table_keys[table]["distkey"] = column
            if row["sortkey1"] > 0:
                table_keys[table]["sortkeys"].append((row["sortkey1"], column))

        for k in table_keys:
            table_keys[k]["sortkeys"] = [col for _, col in sorted(table_keys[k]["sortkeys"])]

    return table_keys

def compare_schemas(schema1, schema2, pk1, pk2, fk1, fk2, keys1, keys2):
    differences = []

    all_tables = set(schema1.keys()).union(schema2.keys())
    for table in all_tables:
        cols1 = schema1.get(table)
        cols2 = schema2.get(table)

        if not cols1:
            differences.append([table, "", "", "", "", "Only in Schema 2"])
            continue
        if not cols2:
            differences.append([table, "", "", "", "", "Only in Schema 1"])
            continue

        max_len = max(len(cols1), len(cols2))
        for i in range(max_len):
            col1 = cols1[i] if i < len(cols1) else None
            col2 = cols2[i] if i < len(cols2) else None

            if col1 and not col2:
                differences.append([table, col1['column_name'], col1['data_type'], "", "", "Missing in Schema 2"])
            elif col2 and not col1:
                differences.append([table, "", "", col2['column_name'], col2['data_type'], "Missing in Schema 1"])
            else:
                mismatch = []
                if col1['column_name'] != col2['column_name']:
                    mismatch.append("Column name mismatch")
                if col1['data_type'] != col2['data_type']:
                    mismatch.append("Data type mismatch")
                if col1['is_nullable'] != col2['is_nullable']:
                    mismatch.append("Nullable mismatch")
                if col1['column_default'] != col2['column_default']:
                    mismatch.append("Default mismatch")

                if mismatch:
                    differences.append([table, col1['column_name'], col1['data_type'],
                                        col2['column_name'], col2['data_type'], "; ".join(mismatch)])

        # Primary Key comparison
        if pk1.get(table) != pk2.get(table):
            differences.append([table, str(pk1.get(table)), "", str(pk2.get(table)), "", "Primary Key mismatch"])

        # Foreign Key comparison
        if fk1.get(table) != fk2.get(table):
            differences.append([table, str(fk1.get(table)), "", str(fk2.get(table)), "", "Foreign Key mismatch"])

        # Sort/Dist key comparison
        k1 = keys1.get(table, {})
        k2 = keys2.get(table, {})
        if k1.get("distkey") != k2.get("distkey"):
            differences.append([table, k1.get("distkey"), "", k2.get("distkey"), "", "DistKey mismatch"])
        if k1.get("sortkeys") != k2.get("sortkeys"):
            differences.append([table, str(k1.get("sortkeys")), "", str(k2.get("sortkeys")), "", "SortKey mismatch"])

    return differences

def write_report(differences, output_file="schema_diff_full.csv"):
    headers = ["Table", "Schema1 Detail", "Schema1 Type", "Schema2 Detail", "Schema2 Type", "Difference"]

    print("\n=== SCHEMA COMPARISON REPORT ===\n")
    print("{:<30} {:<25} {:<20} {:<25} {:<20} {}".format(*headers))
    print("-" * 150)
    for row in differences:
        print("{:<30} {:<25} {:<20} {:<25} {:<20} {}".format(*[str(x) if x is not None else "" for x in row]))

    with open(output_file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(differences)

def print_report(differences):
    headers = ["Table", "Schema1 Detail", "Schema1 Type", "Schema2 Detail", "Schema2 Type", "Difference"]

    print("\n=== 🔍 SCHEMA COMPARISON REPORT ===\n")
    print("{:<30} {:<25} {:<20} {:<25} {:<20} {}".format(*headers))
    print("-" * 150)
    for row in differences:
        print("{:<30} {:<25} {:<20} {:<25} {:<20} {}".format(*[str(x) if x is not None else "" for x in row]))


def main():
    # 🔐 Customize your credentials
    conn1 = psycopg2.connect(
        dbname="db1", user="user1", password="pass1", host="host1", port="5439"
    )
    conn2 = psycopg2.connect(
        dbname="db2", user="user2", password="pass2", host="host2", port="5439"
    )

    schema1 = "public"
    schema2 = "public"

    try:
        cols1 = get_column_details(conn1, schema1)
        cols2 = get_column_details(conn2, schema2)

        def organize_columns(rows):
            schema_map = {}
            for row in rows:
                schema_map.setdefault(row["table_name"], []).append(row)
            return schema_map

        organized1 = organize_columns(cols1)
        organized2 = organize_columns(cols2)

        pk1, fk1 = get_constraints(conn1, schema1)
        pk2, fk2 = get_constraints(conn2, schema2)

        keys1 = get_table_keys(conn1, schema1)
        keys2 = get_table_keys(conn2, schema2)

        diffs = compare_schemas(organized1, organized2, pk1, pk2, fk1, fk2, keys1, keys2)
        write_report(diffs)

        print("\n✅ Comparison done! Output written to 'schema_diff_full.csv'")

    finally:
        conn1.close()
        conn2.close()

if __name__ == "__main__":
    main()
```
