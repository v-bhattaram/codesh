import json
from typing import Union, Dict, Any, List

def generate_draft_mapping_json(
    datapipeline_name: str,
    source_pattern: str,
    target_ddl: Union[str, Dict[str, Any]],
    source_csv_json: Union[str, Dict[str, Any]],
    audit_columns: Dict[str, str]
) -> Dict[str, Any]:
    """
    Generate a draft mapping JSON for source_type='csv_with_header'
    using source columns provided as JSON.

    Rules:
    1) If target col name matches a source column_name (case-insensitive) => tar_col:src_col
    2) If target col name matches audit_columns dict => tar_col:audit (use dict value)
    3) Else => tar_col:pending with source_column_name='PENDING'
    4) For target datatypes date/timestamp/timestamp_tz add default source_column_format.

    Inputs:
      - datapipeline_name: name of pipeline
      - source_pattern: location/file_name_pattern*.csv
      - target_ddl: target DDL JSON (dict or JSON string)
      - source_csv_json: source header JSON (dict or JSON string)
      - audit_columns: dict like {"AUDIT3":"ABC","AUDIT4":"sysdate"}

    Returns:
      - Draft mapping JSON as dict
    """

    # allow passing ddl as json string
    if isinstance(target_ddl, str):
        target_ddl = json.loads(target_ddl)

    # allow passing source header as json string
    if isinstance(source_csv_json, str):
        source_csv_json = json.loads(source_csv_json)

    # default formats for date/timestamp types
    default_formats = {
        "date": "YYYYMMDD",
        "timestamp": "YYYYMMDDHH24MISS",
        "timestamp_tz": "YYYY-MM-DDTHH:MI:SSZ"
    }

    # Extract source column names from JSON
    column_data: List[Dict[str, Any]] = source_csv_json.get("column_data", [])
    src_cols = [c.get("column_name") for c in column_data if c.get("column_name")]
    src_cols_lc = {c.lower(): c for c in src_cols}  # case-insensitive lookup

    # Sort target columns by position, keeping stable order for ties
    details = target_ddl.get("table_details", [])
    details_sorted = sorted(
        enumerate(details),
        key=lambda x: (x[1].get("column_position", 10**9), x[0])
    )

    mapping_details = []
    for _, col in details_sorted:
        tname = col.get("column_name")
        tdatatype = col.get("column_datatype", "string")

        entry = {
            "mapping_type": None,
            "target_column_position": col.get("column_position"),
            "target_column_name": tname,
            "target_column_datatype": tdatatype,
            "target_column_nullable": col.get("column_nullable", "Y"),
        }

        # 1) audit columns win first
        if tname in audit_columns:
            entry["mapping_type"] = "tar_col:audit"
            entry["source_column_name"] = audit_columns[tname]

        # 2) direct match from source header JSON
        elif tname and tname.lower() in src_cols_lc:
            entry["mapping_type"] = "tar_col:src_col"
            entry["source_column_name"] = src_cols_lc[tname.lower()]

        # 3) pending
        else:
            entry["mapping_type"] = "tar_col:pending"
            entry["source_column_name"] = "PENDING"

        # 4) add default format if needed
        if tdatatype in default_formats:
            entry["source_column_format"] = default_formats[tdatatype]

        mapping_details.append(entry)

    return {
        "datapipeline_name": datapipeline_name,
        "source": source_pattern,
        "source_type": "csv_with_header",
        "table_name": target_ddl.get("table_name"),
        "mapping_details": mapping_details
    }



def generate_draft_mapping_json_no_header(
    datapipeline_name: str,
    source_pattern: str,
    target_ddl: Union[str, Dict[str, Any]],
    source_no_header_json: Union[str, Dict[str, Any]],
    audit_columns: Dict[str, str]
) -> Dict[str, Any]:

    # allow passing ddl as json string
    if isinstance(target_ddl, str):
        target_ddl = json.loads(target_ddl)

    # allow passing source json as string
    if isinstance(source_no_header_json, str):
        source_no_header_json = json.loads(source_no_header_json)

    # default formats for date/timestamp types
    default_formats = {
        "date": "YYYYMMDD",
        "timestamp": "YYYYMMDDHH24MISS",
        "timestamp_tz": "YYYY-MM-DDTHH:MI:SSZ"
    }

    # Extract available source positions
    column_data: List[Dict[str, Any]] = source_no_header_json.get("column_data", [])
    src_positions = {
        c.get("column_position") for c in column_data
        if isinstance(c.get("column_position"), int)
    }

    # Sort target columns by position, stable order for ties
    details = target_ddl.get("table_details", [])
    details_sorted = sorted(
        enumerate(details),
        key=lambda x: (x[1].get("column_position", 10**9), x[0])
    )

    mapping_details = []
    for _, col in details_sorted:
        tname = col.get("column_name")
        tpos = col.get("column_position")
        tdatatype = col.get("column_datatype", "string")

        entry = {
            "mapping_type": None,
            "target_column_position": tpos,
            "target_column_name": tname,
            "target_column_datatype": tdatatype,
            "target_column_nullable": col.get("column_nullable", "Y"),
        }

        # 1) audit columns
        if tname in audit_columns:
            entry["mapping_type"] = "tar_col:audit"
            entry["source_column_name"] = audit_columns[tname]

        # 2) positional match (no header)
        elif isinstance(tpos, int) and tpos in src_positions:
            entry["mapping_type"] = "tar_col:src_col_position"
            entry["source_column_name"] = f"column_position_{tpos}"  # draft placeholder

        # 3) pending
        else:
            entry["mapping_type"] = "tar_col:pending"
            entry["source_column_name"] = "PENDING"

        # 4) add default format if needed
        if tdatatype in default_formats:
            entry["source_column_format"] = default_formats[tdatatype]

        mapping_details.append(entry)

    return {
        "datapipeline_name": datapipeline_name,
        "source": source_pattern,
        "source_type": "csv_no_header",
        "table_name": target_ddl.get("table_name"),
        "mapping_details": mapping_details
    }



def generate_draft_mapping_json_fixed_width(
    datapipeline_name: str,
    source_pattern: str,
    target_ddl: Union[str, Dict[str, Any]],
    audit_columns: Dict[str, str]
) -> Dict[str, Any]:
    """
    Generate a draft mapping JSON for fixed-width sources.

    Rules:
    1) Audit target columns => tar_col:audit (use audit_columns value)
    2) All non-audit target columns => tar_col:src_col_substring
       with pending substring [-1, -1] and blank source_column_name.
    3) For target datatypes date/timestamp/timestamp_tz add default source_column_format.
    """

    # allow passing ddl as json string
    if isinstance(target_ddl, str):
        target_ddl = json.loads(target_ddl)


    default_formats = {
        "date": "YYYYMMDD",
        "timestamp": "YYYYMMDDHH24MISS",
        "timestamp_tz": "YYYY-MM-DDTHH:MI:SSZ"
    }

    details: List[Dict[str, Any]] = target_ddl.get("table_details", [])
    details_sorted = sorted(
        enumerate(details),
        key=lambda x: (x[1].get("column_position", 10**9), x[0])
    )

    mapping_details = []
    for _, col in details_sorted:
        tname = col.get("column_name")
        tpos = col.get("column_position")
        tdatatype = col.get("column_datatype", "string")

        entry = {
            "mapping_type": None,
            "target_column_position": tpos,
            "target_column_name": tname,
            "target_column_datatype": tdatatype,
            "target_column_nullable": col.get("column_nullable", "Y"),
        }

        # Audit columns
        if tname in audit_columns:
            entry["mapping_type"] = "tar_col:audit"
            entry["source_column_name"] = audit_columns[tname]

        # Everything else pending substring
        else:
            entry["mapping_type"] = "tar_col:src_col_substring"
            entry["source_column_name"] = ""          # blank
            entry["source_column_substring"] = [-1, -1]  # pending

        # Add default format if needed
        if tdatatype in default_formats:
            entry["source_column_format"] = default_formats[tdatatype]

        mapping_details.append(entry)

    return {
        "datapipeline_name": datapipeline_name,
        "source": source_pattern,
        "source_type": "fixed_width",
        "table_name": target_ddl.get("table_name"),
        "mapping_details": mapping_details
    }
