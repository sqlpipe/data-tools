# cloudRunCsvCleaner

Cleans and normalizes CSV data from a mounted filesystem, writes the cleaned CSV to Google Cloud Storage, and emits a matching BigQuery schema JSON. Designed to be used within GCP Cloud Run Jobs, and triggered by Airflow.

## Creating a job

I recommend creating a job in Airflow, instead of the console. It's not too hard. Here's an example pattern that checks if a CSV cleaner job exists inside Cloud Run. If it does, it updates it to use the latest job spec. If not, it creates it:

```python
csv_cleaner_job_spec = {
    "labels": {
        "optional-labels-to-track": "how-much-stuff-costs"
    },
    "template": {
        "template": {
            "containers": [
                {
                    "image": "sqlpipe/cloud-run-csv-cleaner:3",
                    "resources": {
                        "limits": {
                            "cpu": "1",
                            "memory": "512Mi",
                        }
                    },
                    "volume_mounts": [
                        {
                            "name": "gcs-storage",
                            "mount_path": "/gcs",
                        }
                    ],
                    "env": [
                        {
                            "name": "MOUNT_PATH",
                            "value": "/gcs",
                        },
                        {
                            "name": "BUCKET",
                            "value": GCS_BUCKET,
                        },
                        {
                            "name": "DAG_ID",
                            "value": {{ dag.dag_id }},
                        },
                        {
                            "name": "MAKE_COLUMN_NAMES_SQL_FRIENDLY",
                            "value": "true",
                        },
                        {
                            "name": "REMOVE_VALUES",
                            "value": "NaN",
                        },
                        {
                            "name": "DELETE_IN_FILE",
                            "value": "false",
                        },
                    ],
                }
            ],
            "timeout": "3600s",
            "volumes": [
                {
                    "name": "gcs-storage",
                    "gcs": {
                        "bucket": "your-gcs-bucket-name",
                    },
                }
            ],
        },
    },
}

def branch_on_csv_cleaner_job_existence(**_):
    hook = CloudRunHook(gcp_conn_id="google_cloud_default")
    try:
        hook.get_job(job_name=CSV_CLEANER_JOB, region=REGION, project_id=PROJECT_ID)
        return "update-csv-cleaner-job-task"
    except NotFound:
        return "create-csv-cleaner-job-task"

branch_on_csv_cleaner_job_existence_task = BranchPythonOperator(
    task_id="branch-on-csv-cleaner-job-existence-task",
    python_callable=branch_on_csv_cleaner_job_existence,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)

update_csv_cleaner_job_task = CloudRunUpdateJobOperator(
    task_id="update-csv-cleaner-job-task",
    project_id=PROJECT_ID,
    region=REGION,
    job_name=CSV_CLEANER_JOB,
    job=csv_cleaner_job_spec,
)

create_csv_cleaner_job_task = CloudRunCreateJobOperator(
    task_id="create-csv-cleaner-job-task",
    project_id=PROJECT_ID,
    region=REGION,
    job_name=CSV_CLEANER_JOB,
    job=csv_cleaner_job_spec,
)

overrides = {
    "container_overrides": [
        {
            "env": [
                {
                    "name": "IN_FILE",
                    "value": "path/to/input.csv",
                },
                {
                    "name": "OUT_FILE",
                    "value": "clean/path/to/output.csv",
                },
                {
                    "name": "SCHEMA_FILE",
                    "value": "clean/path/to/output.schema.json",
                },
                {
                    "name": "TASK_ID",
                    "value": "name-you-want-the-task-to-have",
                },
                {
                    "name": "SCHEMA_TRANSFORM",
                    "value": [
                      {"expected-column-name": "SOURCE_REF:Publication Source", "type": "string", "rename-to": "publication_source"},
                      {"expected-column-name": "SUPP_INFO_BREAKS:Supplemental information and breaks", "type": "string", "rename-to": "supplemental_information_and_breaks"},
                    ]
                },
                {
                    "name": "EXPORT_HEAD",
                    "value": "false",
                },
                {
                    "name": "EXPORT_NIL_TRANSFORM",
                    "value": "false",
                },
            ]
        }
    ]
}

execute_csv_cleaner_job_task = CloudRunExecuteJobOperator(
    task_id="execute-csv-cleaner-job-task-id",
    project_id=PROJECT_ID,
    region=REGION,
    job_name=CSV_CLEANER_JOB,
    overrides=overrides,
    retries=1,
    retry_delay=timedelta(seconds=60),
    retry_exponential_backoff=True,
    deferrable=DEFERRABLE,
)
```

## Environment variables

- MOUNT_PATH (required): Absolute path to the mounted writable directory inside the container.
- IN_FILE (required): Relative path (under MOUNT_PATH) to the input CSV file.
- OUT_FILE (required): Destination GCS object path for the cleaned CSV (e.g., `clean/my.csv`).
- SCHEMA_FILE (required): Destination GCS object path for the BigQuery schema JSON (e.g., `clean/my.schema.json`).
- BUCKET (required): Target GCS bucket name.
- TASK_ID (required): Task identifier used for structured logging context.
- DAG_ID (required): DAG/flow identifier used for structured logging context.
- MAKE_COLUMN_NAMES_SQL_FRIENDLY (optional): When "true", converts header names to SQL‑friendly identifiers.
- REMOVE_VALUES (optional): Comma‑separated list of exact cell values to blank out during processing.
- EXPORT_HEAD (optional): When "true", writes a header + first 5 data rows preview next to the cleaned CSV (`OUT_FILE + ".head.csv"`).
- EXPORT_NIL_TRANSFORM (optional): When "true", writes `nil-transform.json` next to `SCHEMA_FILE`, describing a no‑op header/type mapping.
- DELETE_IN_FILE (optional): When "true", deletes the local input file after successful processing.
- SCHEMA_TRANSFORM (optional): JSON array describing an explicit schema and header mapping. When set, the header is transformed and all values are validated against the provided types.

## Behavior

- Validates required environment variables at startup; exits non‑zero on missing values.
- Reads the first line as a raw header string, optionally applies `SCHEMA_TRANSFORM` substring replacements, then parses the resulting header as CSV.
- If `SCHEMA_TRANSFORM` is provided:
  - Validates that the number/order of headers matches the transform definition (after replacement/renaming).
  - Validates each non‑empty cell against the specified column type; mismatch → immediate non‑zero exit.
- If `SCHEMA_TRANSFORM` is not provided:
  - Optionally makes column names SQL‑friendly (`MAKE_COLUMN_NAMES_SQL_FRIENDLY`).
  - Infers primitive types per column across all rows (Integer → Float → Boolean → String fallback).
- Applies `REMOVE_VALUES` by replacing exact matches with empty strings before validation/inference.
- Writes cleaned CSV to `gs://BUCKET/OUT_FILE`.
- Builds a BigQuery schema JSON (array of `{ "name", "type", "mode": "NULLABLE" }`) and writes it to `gs://BUCKET/SCHEMA_FILE`.
- If `EXPORT_HEAD` is true, writes a preview to `gs://BUCKET/<OUT_FILE>.head.csv`.
- If `EXPORT_NIL_TRANSFORM` is true, writes `nil-transform.json` next to the schema file, mapping original headers to final names and mapping BigQuery types back to transform‑style types (`string`, `number`, `boolean`).
- If `DELETE_IN_FILE` is true, removes the local input file.
- Emits structured logs with `task-id` and `dag-id` and progress lines every 100,000 rows.
- Exits with code 0 on success.
