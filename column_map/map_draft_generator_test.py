import map_draft_generator as mdg
import json
from typing import Any, Dict, Union

import json
from typing import Any, Union, Dict, List

def write_json_file(
    data: Union[Dict[str, Any], List[Any]],
    file_path: str,
    indent: int = 2,
    sort_keys: bool = False
) -> None:
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(
                data,
                f,
                indent=indent,
                sort_keys=sort_keys,
                ensure_ascii=False
            )
            f.write("\n")  # nice ending newline
    except Exception as e:
        raise IOError(f"Failed to write JSON to {file_path}: {e}")


def read_json_file(file_path: str) -> Union[Dict[str, Any], list]:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"JSON file not found: {file_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in file {file_path}: {e}")


# ##########################################################
#
l_audit_columns = {"AUDIT3":"ABC","AUDIT4":"sysdate"}
target_ddl_json_path = r"F:\DSW\datapipeline-framework\column_map\input\table_ddl_json.json"
source_csv_with_header_json_path = r"F:\DSW\datapipeline-framework\column_map\input\source_csv_header.json"
l_datapipeline_name="rd001"

draft = mdg.generate_draft_mapping_json(
     datapipeline_name=l_datapipeline_name
    ,source_pattern="/data/landing/abc_*.csv"
    ,target_ddl=read_json_file(target_ddl_json_path)
    ,source_csv_json=read_json_file(source_csv_with_header_json_path)
    ,audit_columns=l_audit_columns
)

draft_file_path=r"F:\DSW\datapipeline-framework\column_map\input\draft_column_map_"+l_datapipeline_name+".json"
write_json_file(draft, draft_file_path)
print(draft)
#
# ##########################################################


# ##########################################################
#
l_audit_columns = {"AUDIT3":"ABC","AUDIT4":"sysdate"}
target_ddl_json_path = r"F:\DSW\datapipeline-framework\column_map\input\table_ddl_json.json"
source_csv_no_header_json_path = r"F:\DSW\datapipeline-framework\column_map\input\source_csv_no_header.json"
l_datapipeline_name="rd002"

draft = mdg.generate_draft_mapping_json_no_header(
     datapipeline_name = l_datapipeline_name
    ,source_pattern = "/data/landing/abc_*.csv"
    ,target_ddl = read_json_file(target_ddl_json_path)
    ,source_no_header_json = read_json_file(source_csv_no_header_json_path)
    ,audit_columns = l_audit_columns
)

draft_file_path=r"F:\DSW\datapipeline-framework\column_map\input\draft_column_map_"+l_datapipeline_name+".json"
write_json_file(draft, draft_file_path)
print(draft)
#
# ##########################################################

l_audit_columns = {"AUDIT3":"ABC","AUDIT4":"sysdate"}
target_ddl_json_path = r"F:\DSW\datapipeline-framework\column_map\input\table_ddl_json.json"
source_csv_no_header_json_path = r"F:\DSW\datapipeline-framework\column_map\input\source_csv_no_header.json"
l_datapipeline_name="rd003"

draft = mdg.generate_draft_mapping_json_fixed_width(
     datapipeline_name = l_datapipeline_name
    ,source_pattern = "/data/landing/abc_*.csv"
    ,target_ddl = read_json_file(target_ddl_json_path)
    ,audit_columns = l_audit_columns
)

draft_file_path=r"F:\DSW\datapipeline-framework\column_map\input\draft_column_map_"+l_datapipeline_name+".json"
write_json_file(draft, draft_file_path)
print(draft)
