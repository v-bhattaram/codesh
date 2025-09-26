# Python Data Pipeline Framework 

## 1. Overview
The Data Pipeline Framework, automates the creation and execution of ETL (Extract–Transform–Load) pipelines.  
It is **Database table** and script **template-driven** framework, where datapipelines are generated based on a reusable reusable templates.

## 2. Components
### Configuration and Templates
#### Folder Structure and GIT Repository
* Folder Structure
```
/intg-datapipeline
    /common
    /core
    /dags
        /subject-area/integration
            /workflow
                dag-scripts.py
    /utils
    /subject-area/integration
        /workflow
            /scripts
                etl-scripts.py
            /config
    /templates
        /etl-type
            etl-template.py
            etl-utils.py             
```
* The `common`,`core`,`utils` are supporting folders/python files for the etl process
* `dags` are the generated DAG python scripts folder with subject-area and workflow subfolders
* 
#### Postgresql
## 2. Data Model

The framework uses the following tables:

- **`datapipeline_manager`** — master metadata for pipelines  
- **`datapipeline_template`** — defines reusable template folders & handlers  
- **`datapipeline_run`** — captures execution runs  
- **`datapipeline_runlog`** — detailed logs per run  
- **`datapipeline_errorlog`** — errors encountered during execution  

---

### 2.1 `datapipeline_manager`

Stores pipeline definitions.

| Column              | Type          | Description                                                                 |
|----------------------|---------------|-----------------------------------------------------------------------------|
| `datapipeline_id`    | INT (PK)      | Unique identifier for the pipeline.                                         |
| `datapipeline_name`  | VARCHAR       | Logical name of the pipeline.                                               |
| `handler_path`       | VARCHAR       | Filesystem path where pipeline scripts will be generated.                   |
| `handler_script_name`| VARCHAR       | Generated Python script name.                                               |
| `notes`              | VARCHAR/TEXT  | Free-form notes for documentation.                                          |
| `run_frequency`      | VARCHAR       | Frequency (daily, hourly, weekly, etc.).                                    |
| `parms_template_id`  | INT (FK)      | Reference to `datapipeline_template`.                                       |
| `status`             | VARCHAR       | `ACTIVE` / `DISABLED`.                                                      |
| `created_by`         | VARCHAR       | Audit info.                                                                 |
| `created_date`       | TIMESTAMP     | Audit info.                                                                 |
| `updated_by`         | VARCHAR       | Audit info.                                                                 |
| `updated_date`       | TIMESTAMP     | Audit info.                                                                 |

---

### 2.2 `datapipeline_template`

Stores reusable template metadata.

| Column                | Type          | Description                                                                 |
|------------------------|---------------|-----------------------------------------------------------------------------|
| `parms_template_id`    | INT (PK)      | Unique identifier for the template.                                         |
| `template_name`        | VARCHAR       | Name of the template (folder).                                              |
| `template_path`        | VARCHAR       | Filesystem path to the template folder.                                     |
| `base_script`          | VARCHAR       | Name of base ETL script (e.g., `base_etl.py`).                              |
| `dag_script`           | VARCHAR       | Name of Airflow DAG script (e.g., `airflow-dag-etl.py`).                    |
| `default_params_file`  | VARCHAR       | Default params file (e.g., `params.cnf`).                                   |
| `notes`                | VARCHAR/TEXT  | Optional documentation.                                                     |
| `created_by`           | VARCHAR       | Audit info.                                                                 |
| `created_date`         | TIMESTAMP     | Audit info.                                                                 |
| `updated_by`           | VARCHAR       | Audit info.                                                                 |
| `updated_date`         | TIMESTAMP     | Audit info.                                                                 |

---

### 2.3 `datapipeline_run`

Stores each execution instance.

| Column               | Type          | Description                                                                 |
|-----------------------|---------------|-----------------------------------------------------------------------------|
| `datapipeline_run_id`| INT (PK)      | Unique ID for each run.                                                     |
| `rundatetime`         | TIMESTAMP     | When the run started.                                                       |
| `message`             | TEXT          | Summary message of the run.                                                 |
| `status`              | VARCHAR       | `SUCCESS`, `FAILED`, `RUNNING`.                                             |
| `datapipeline_id`     | INT (FK)      | Reference to `datapipeline_manager`.                                        |
| `created_by`          | VARCHAR       | Audit info.                                                                 |
| `created_date`        | TIMESTAMP     | Audit info.                                                                 |
| `updated_by`          | VARCHAR       | Audit info.                                                                 |
| `updated_date`        | TIMESTAMP     | Audit info.                                                                 |

---

### 2.4 `datapipeline_runlog`

Detailed logs per run.

| Column                  | Type          | Description                                                                 |
|--------------------------|---------------|-----------------------------------------------------------------------------|
| `datapipeline_runlog_id` | INT (PK)      | Unique ID for the run log entry.                                            |
| `message`                | TEXT          | Detailed log message.                                                       |
| `datapipeline_id`        | INT (FK)      | Reference to `datapipeline_manager`.                                        |
| `created_by`             | VARCHAR       | Audit info.                                                                 |
| `created_date`           | TIMESTAMP     | Audit info.                                                                 |
| `updated_by`             | VARCHAR       | Audit info.                                                                 |
| `updated_date`           | TIMESTAMP     | Audit info.                                                                 |

---

### 2.5 `datapipeline_errorlog`

Captures error messages from pipelines.

| Column                    | Type          | Description                                                                 |
|----------------------------|---------------|-----------------------------------------------------------------------------|
| `datapipeline_errorlog_id` | INT (PK)      | Unique ID for the error log entry.                                          |
| `message`                  | TEXT          | Error details.                                                              |
| `datapipeline_id`          | INT (FK)      | Reference to `datapipeline_manager`.                                        |
| `created_by`               | VARCHAR       | Audit info.                                                                 |
| `created_date`             | TIMESTAMP     | Audit info.                                                                 |
| `updated_by`               | VARCHAR       | Audit info.                                                                 |
| `updated_date`             | TIMESTAMP     | Audit info.                                                                 |

---

#### Python Template Scripts

#### Custom Libraries



#### Azure DevOps


## 3. Run Process, Logging and Monitoring


## 4. Developers Guide





- A base ETL script (`etl.py`)  
- An Airflow DAG wrapper (`airflow-dag-etl.py`)  
- A default parameters configuration (`params.cnf`)  

Pipelines are managed centrally using a **`datapipeline_manager`** table.  
The table stores metadata about pipelines, including:

- **Pipeline name**
- **Configuration values**
- **Target script filename**
- **Template to use**

The framework generates the pipeline Python file by copying from a template and linking it with a parameters file.

---

## 2. Data Model

### `datapipeline_manager` table

| Column Name          | Type          | Description                                                                 |
|-----------------------|---------------|-----------------------------------------------------------------------------|
| `pipeline_id`         | INT (PK)      | Unique identifier for each pipeline.                                        |
| `pipeline_name`       | VARCHAR       | Logical name of the pipeline.                                               |
| `template_name`       | VARCHAR       | Name of the template folder (e.g., `sales_etl_template`).                   |
| `generated_filename`  | VARCHAR       | Output Python script filename (e.g., `sales_pipeline.py`).                  |
| `config_json`         | JSON / TEXT   | JSON-based pipeline configuration (connection info, SQL, API keys, etc.).   |
| `params_file`         | VARCHAR       | Path to params file (auto-generated by another script).                     |
| `status`              | ENUM/CHAR     | Status of pipeline (`ACTIVE`, `DISABLED`, etc.).                            |
| `created_at`          | TIMESTAMP     | Audit info.                                                                 |
| `updated_at`          | TIMESTAMP     | Audit info.                                                                 |

---

## 3. Template Structure
Each **template folder** must follow a standard structure:

