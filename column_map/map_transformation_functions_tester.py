"""
test_transforms.py

Demonstrates the vectorized transform functions from transforms.py.
Creates sample DataFrames inside this file (no external files).
"""

import pandas as pd
import numpy as np

from map_transformation_functions import (
    _fn_upper,
    _fn_lower,
    _fn_concat,
    _fn_substring,
    _fn_to_date,
    _fn_cast,
    _fn_coalesce
)


def main():
    # -----------------------------
    # Build sample DataFrame
    # -----------------------------
    df = pd.DataFrame({
        # USA cities (mixed case + None for testing)
        "name": ["new york", "LoS aNGeLeS", None, "ChiCaGo"],
        "code": ["ABCDEF", "xyzpqr", "LMN123", None],
        "date_yyyymmdd": ["20250101", "20250215", None, "20250331"],
        "date_dash": ["2025-01-01", "2025-02-15", "bad_date", None],
        "num_str": ["10", "20.5", None, "bad"],
        "a": ["A1", None, "A3", "A4"],
        "b": [None, "B2", None, "B4"]
    })

    print("\n=== INPUT DF ===")
    print(df)

    # -----------------------------
    # 1) _fn_upper
    # -----------------------------
    upper_name = _fn_upper(df["name"])
    print("\n=== _fn_upper(name) ===")
    print(upper_name)

    # -----------------------------
    # 2) _fn_lower
    # -----------------------------
    lower_name = _fn_lower(df["name"])
    print("\n=== _fn_lower(name) ===")
    print(lower_name)

    # -----------------------------
    # 3) _fn_concat
    # -----------------------------
    concat_code_num = _fn_concat(df["code"], pd.Series(["-"] * len(df)), df["num_str"])
    print("\n=== _fn_concat(code, '-', num_str) ===")
    print(concat_code_num)

    # -----------------------------
    # 4) _fn_substring
    # -----------------------------
    sub_code = _fn_substring(df["code"], 0, 3)
    print("\n=== _fn_substring(code, 0, 3) ===")
    print(sub_code)

    # -----------------------------
    # 5) _fn_to_date
    # -----------------------------
    to_date_yyyymmdd = _fn_to_date(df["date_yyyymmdd"], "yyyymmdd")
    print("\n=== _fn_to_date(date_yyyymmdd, 'yyyymmdd') ===")
    print(to_date_yyyymmdd)

    to_date_dash = _fn_to_date(df["date_dash"], "yyyy-mm-dd")
    print("\n=== _fn_to_date(date_dash, 'yyyy-mm-dd') ===")
    print(to_date_dash)

    # -----------------------------
    # 6) _fn_cast
    # -----------------------------
    cast_num = _fn_cast(df["num_str"], "number")
    print("\n=== _fn_cast(num_str, 'number') ===")
    print(cast_num)

    cast_int = _fn_cast(df["num_str"], "int")
    print("\n=== _fn_cast(num_str, 'int') ===")
    print(cast_int)

    cast_date = _fn_cast(df["date_dash"], "date")
    print("\n=== _fn_cast(date_dash, 'date') ===")
    print(cast_date)




    # -----------------------------
    # 7) _fn_coalesce
    # -----------------------------
    coalesced_ab = _fn_coalesce(df["a"], df["b"])
    print("\n=== _fn_coalesce(a, b) ===")
    print(coalesced_ab)

    # -----------------------------
    # Quick sanity asserts
    # -----------------------------
    assert upper_name.iloc[0] == "NEW YORK"
    assert lower_name.iloc[1] == "los angeles"
    assert concat_code_num.iloc[0] == "ABCDEF-10"
    assert sub_code.iloc[0] == "ABC"
    assert str(to_date_yyyymmdd.iloc[0]) == "2025-01-01"
    assert float(cast_num.iloc[1]) == 20.5
    assert coalesced_ab.iloc[1] == "B2"

    print("\nAll tests passed ✅")


if __name__ == "__main__":
    main()
