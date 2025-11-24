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

def demo_fn_usage_batch_write():
    # -----------------------------
    # Source DF (built here)
    # -----------------------------
    df_source = pd.DataFrame({
        "first": ["john", "jane", None],
        "last":  ["doe",  "smith", "lee"],
        "city":  ["new york", "los angeles", "chicago"],
        "code":  ["ABCDEF", "xyzpqr", "LMN123"],
        "date_yyyymmdd": ["20250101", "20250215", None],
        "num_str": ["10", "20.5", "bad"],
        "a": ["A1", None, "A3"],
        "b": [None, "B2", None]
    })

    # -----------------------------
    # Row-level transforms (vectorized)
    # USING your _fn_ functions
    # -----------------------------
    full_name = _fn_concat(
        _fn_upper(df_source["first"].fillna("")),
        pd.Series([" "] * len(df_source)),
        _fn_upper(df_source["last"].fillna(""))
    )

    code_prefix = _fn_substring(df_source["code"], 0, 3)

    load_date = _fn_to_date(df_source["date_yyyymmdd"], "yyyymmdd")

    amt_num = _fn_cast(df_source["num_str"], "number")  # float-like

    coalesced_ab = _fn_coalesce(df_source["a"], df_source["b"])

    # Example of row-level conditional using results of _fn_ outputs
    label = np.where(
        _fn_lower(df_source["city"]).str.contains("los", na=False),
        "WEST",
        "OTHER"
    )

    # -----------------------------
    # Build target DF ONCE (batch write)
    # -----------------------------
    computed = {
        "full_name": full_name,
        "code_prefix": code_prefix,
        "load_date": load_date,
        "amt_num": amt_num,
        "coalesced_ab": coalesced_ab,
        "label": label
    }

    df_target = pd.DataFrame(computed, index=df_source.index)

    print("=== SOURCE ===")
    print(df_source)
    print("\n=== TARGET (built once) ===")
    print(df_target)

demo_fn_usage_batch_write()
