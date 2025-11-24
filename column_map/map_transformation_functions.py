# transforms.py
# Vectorized pandas transform functions used by the mapping framework.

import pandas as pd
import numpy as np
from typing import Any


def _fn_upper(x: pd.Series) -> pd.Series:
    """UPPER(source_col)"""
    return x.astype(str).str.upper()


def _fn_lower(x: pd.Series) -> pd.Series:
    """LOWER(source_col)"""
    return x.astype(str).str.lower()


def _fn_concat(*args: pd.Series) -> pd.Series:
    """
    CONCAT(a, b, c, ...)
    - Converts all args to strings
    - Treats NaN as ''
    - Vectorized
    """
    out = None
    for a in args:
        s = a.astype(str).fillna("")
        out = s if out is None else (out + s)
    return out


def _fn_substring(x: pd.Series, start: int, end: int) -> pd.Series:
    """SUBSTRING(source_col, start, end) -> source_col[start:end]"""
    return x.astype(str).str.slice(start, end)


def _fn_to_date(x: pd.Series, fmt: str) -> pd.Series:
    """
    TO_DATE(source_col, format)
    Supported friendly formats:
      - yyyymmdd
      - yyyy-mm-dd
      - yyyy/mm/dd
      - yyyyMMddHHMMSS   (datetime)
    If fmt is not in map, it is treated as a python strptime format.
    """
    fmt_map = {
        "yyyymmdd": "%Y%m%d",
        "yyyy-mm-dd": "%Y-%m-%d",
        "yyyy/mm/dd": "%Y/%m/%d",
        "yyyymmddhhmmss": "%Y%m%d%H%M%S",
        "yyyyMMddHHMMSS": "%Y%m%d%H%M%S"
    }

    key = fmt.lower() if isinstance(fmt, str) else fmt
    pyfmt = fmt_map.get(key, fmt)

    return pd.to_datetime(x, format=pyfmt, errors="coerce").dt.date


def _fn_cast(x: pd.Series, dtype: str) -> pd.Series:
    """
    CAST(source_col AS dtype)

    dtype examples:
      - string
      - int / int64
      - float / float64
      - number (alias → float64)
      - date
      - timestamp / datetime
    """
    dt = dtype.lower()

    if dt in ("string", "str"):
        return x.astype(str)

    if dt in ("number",):
        return pd.to_numeric(x, errors="coerce")

    if dt in ("int", "int64", "integer"):
        # 1) numeric coercion
        num = pd.to_numeric(x, errors="coerce")
        # 2) make integer-valued before casting (prevents safe-cast error)
        num = num.round(0)   # or use np.floor(num) if you want floor behavior
        # 3) nullable integer
        return num.astype("Int64")

    if dt in ("float", "float64", "double"):
        return pd.to_numeric(x, errors="coerce").astype(float)

    if dt in ("date",):
        return pd.to_datetime(x, errors="coerce").dt.date

    if dt in ("timestamp", "datetime"):
        return pd.to_datetime(x, errors="coerce")

    # fallback to pandas astype
    return x.astype(dtype)



def _fn_coalesce(*args: pd.Series) -> pd.Series:
    """
    COALESCE(a, b, c, ...)
    Returns first non-null value across args.
    """
    if not args:
        return pd.Series(dtype="object")

    out = args[0]
    for a in args[1:]:
        out = out.combine_first(a)
    return out
