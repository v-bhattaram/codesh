import pandas as pd
import numpy as np
from datetime import datetime
from decimal import Decimal
import os


p_column_datatype_list = [
  { "col_name": "id",              "datatype": "int" },
  { "col_name": "name",            "datatype": "string" },
  { "col_name": "age",             "datatype": "int" },
  { "col_name": "signup_date",     "datatype": "date" },
  { "col_name": "last_login",      "datatype": "datetime" },
  { "col_name": "purchase_ts_tz",  "datatype": "datetime_with_timezone" },
  { "col_name": "balance",         "datatype": "decimal" },
  { "col_name": "rating",          "datatype": "float" },
  { "col_name": "country",         "datatype": "string" },
  { "col_name": "city",            "datatype": "string" },
  { "col_name": "zipcode",         "datatype": "int" },
  { "col_name": "flag",            "datatype": "boolean" },
  { "col_name": "created_dt",      "datatype": "datetime" },
  { "col_name": "updated_dt_tz",   "datatype": "datetime_with_timezone" },
  { "col_name": "score",           "datatype": "decimal" }
]




def validate_and_split_csv(
    input_file: str,
    column_datatype_list: list,
    good_file: str = "good.csv",
    bad_file: str = "bad.csv",
    chunksize: int = 50_000
):
    """
    Validate datatypes of a large CSV and split into good/bad rows.

    Parameters
    ----------
    input_file : str
        Path to the input CSV file.
    column_datatype_list : list[dict]
        List of { "col_name": <name>, "datatype": <type> }
    """

    # Convert list of dicts to a single mapping
    type_map = {c["col_name"]: c["datatype"] for c in column_datatype_list}

    # Remove old outputs if present
    for f in (good_file, bad_file):
        if os.path.exists(f):
            os.remove(f)

    def is_valid(value, dtype):
        if pd.isna(value) or (isinstance(value, str) and value.strip() == ""):
            return True
        try:
            if dtype == "int":
                int(value)
            elif dtype == "float":
                float(value)
            elif dtype == "decimal":
                Decimal(str(value))
            elif dtype == "string":
                str(value)
            elif dtype == "date":
                pd.to_datetime(value, format="%Y-%m-%d", errors="raise").date()
            elif dtype == "datetime":
                pd.to_datetime(value, errors="raise")
            elif dtype == "datetime_with_timezone":
                pd.to_datetime(value, utc=True, errors="raise")
            elif dtype == "boolean":
                if str(value).lower() not in {"true", "false", "1", "0"}:
                    raise ValueError
            else:
                return False
            return True
        except Exception:
            return False

    for chunk in pd.read_csv(input_file, chunksize=chunksize, dtype=str, keep_default_na=False):
        good_rows, bad_rows = [], []

        for _, row in chunk.iterrows():
            issues = []
            for col, dtype in type_map.items():
                val = row.get(col, "")
                if not is_valid(val, dtype):
                    issues.append(f"{col}:expected_{dtype}")

            if issues:
                r = row.copy()
                r["reject_reason"] = "; ".join(issues)
                bad_rows.append(r)
            else:
                good_rows.append(row)

        if good_rows:
            pd.DataFrame(good_rows).to_csv(
                good_file, mode="a", header=not os.path.exists(good_file), index=False
            )
        if bad_rows:
            pd.DataFrame(bad_rows).to_csv(
                bad_file, mode="a", header=not os.path.exists(bad_file), index=False
            )

    print(f"Validation complete.\nGood rows → {good_file}\nBad rows  → {bad_file}")


validate_and_split_csv (
    "E:/DSW/synthetic_data.csv",
    p_column_datatype_list
)