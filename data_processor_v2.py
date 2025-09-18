import pandas as pd
import numpy as np
import os
from decimal import Decimal
from datetime import datetime

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


def validate_and_split_csv_fast(
    input_file: str,
    column_datatype_list: list,
    good_file: str = "good.csv",
    bad_file: str = "bad.csv",
    chunksize: int = 100_000   # larger chunk reduces overhead
):
    # Precompute mapping
    type_map = {c["col_name"]: c["datatype"] for c in column_datatype_list}

    # Clean up once
    for f in (good_file, bad_file):
        if os.path.isfile(f):
            os.remove(f)

    # Precompile simple validators for speed
    def make_validator(dtype):
        if dtype == "int":
            return lambda v: v == "" or v.isdigit() or (v.startswith("-") and v[1:].isdigit())
        elif dtype == "float":
            return lambda v: v == "" or _is_float(v)
        elif dtype == "decimal":
            return lambda v: v == "" or _is_decimal(v)
        elif dtype == "string":
            return lambda v: True
        elif dtype == "date":
            return lambda v: v == "" or _is_date(v)
        elif dtype == "datetime":
            return lambda v: v == "" or _is_datetime(v)
        elif dtype == "datetime_with_timezone":
            return lambda v: v == "" or _is_datetime_tz(v)
        elif dtype == "boolean":
            return lambda v: v == "" or v.lower() in {"true", "false", "1", "0"}
        else:
            return lambda v: False

    # Lightweight parsers
    def _is_float(x):
        try: float(x); return True
        except: return False
    def _is_decimal(x):
        try: Decimal(x); return True
        except: return False
    def _is_date(x):
        try: pd.to_datetime(x, format="%Y-%m-%d", errors="raise"); return True
        except: return False
    def _is_datetime(x):
        try: pd.to_datetime(x, errors="raise"); return True
        except: return False
    def _is_datetime_tz(x):
        try: pd.to_datetime(x, utc=True, errors="raise"); return True
        except: return False

    validators = {col: make_validator(dt) for col, dt in type_map.items()}

    # Stream read with vectorization
    for chunk in pd.read_csv(input_file, chunksize=chunksize, dtype=str, keep_default_na=False):
        chunk = chunk.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        invalid_masks = []
        for col, validate in validators.items():
            mask = ~chunk[col].apply(validate)
            invalid_masks.append(mask)

        combined_bad_mask = np.logical_or.reduce(invalid_masks)
        bad_rows = chunk[combined_bad_mask].copy()

        if not bad_rows.empty:
            # Build reject_reason only for the *bad_rows* subset
            reasons_per_bad_row = []
            for idx, bad_row in bad_rows.iterrows():
                # gather failed columns for this row
                issues = [
                    f"{col}:expected_{type_map[col]}"
                    for col, validate in validators.items()
                    if not validate(bad_row[col])
                ]
                reasons_per_bad_row.append("; ".join(issues))
            bad_rows["reject_reason"] = reasons_per_bad_row

        good_rows = chunk[~combined_bad_mask]

        if not good_rows.empty:
            good_rows.to_csv(good_file, mode="a",
                            header=not os.path.exists(good_file), index=False)
        if not bad_rows.empty:
            bad_rows.to_csv(bad_file, mode="a",
                            header=not os.path.exists(bad_file), index=False)


    print(f"Validation complete.\nGood rows → {good_file}\nBad rows  → {bad_file}")


start_time = datetime.now()
print(f"Validation started at: {start_time}")

validate_and_split_csv_fast(
    "E:/DSW/synthetic_data.csv",
    p_column_datatype_list
)

end_time = datetime.now()
print(f"Validation ended at: {end_time}")