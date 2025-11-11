# cloudRunUnzipInGcs

Unzips a file from a mounted filesystem and uploads its contents to Google Cloud Storage under a specified object prefix. Designed to be used within GCP Cloud Run Jobs, and triggered by Airflow.

## Creating a job

I recommend creating a job in Airflow, instead of the console. It's not too hard. Here's an example pattern that checks if an unzip job exists inside Cloud Run. If it does, it updates it to use the latest job spec. If not, it creates it:

```python
unzip_job_spec = {
    "labels": {
        "optional-labels-to-track": "how-much-stuff-costs"
    },
    "template": {
        "template": {
            "containers": [
                {
                    "image": UNZIP_IMAGE_WITH_TAG,
                    "resources": {
                        "limits": {
                            "cpu": "1",
                            "memory": "512Mi",
                        }
                    },
                    "volume_mounts": [
                        {
                            "name": "gcs-storage",
                            "mount_path": MOUNT_PATH,
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
                            "name": "DELETE_ZIP_FILE",
                            "value": "true",
                        },
                    ],
                }
            ],
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

def branch_on_unzip_job_existence(**_):
    hook = CloudRunHook(gcp_conn_id="google_cloud_default")
    try:
        hook.get_job(job_name=UNZIP_JOB, region=REGION, project_id=PROJECT_ID)
        return "update-unzip-job-task"
    except NotFound:
        return "create-unzip-job-task"

branch_on_unzip_job_existence_task = BranchPythonOperator(
    task_id="branch-on-unzip-job-existence-task",
    python_callable=branch_on_unzip_job_existence,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)

update_unzip_job_task = CloudRunUpdateJobOperator(
    task_id="update-unzip-job-task",
    project_id=PROJECT_ID,
    region=REGION,
    job_name=UNZIP_JOB,
    job=unzip_job_spec,
)

create_unzip_job_task = CloudRunCreateJobOperator(
    task_id="create-unzip-job-task",
    project_id=PROJECT_ID,
    region=REGION,
    job_name=UNZIP_JOB,
    job=unzip_job_spec,
)

overrides = {
    "container_overrides": [
        {
            "env": [
                {
                    "name": "IN_FILE",
                    "value": "file-to-unzip.zip",
                },
                {
                    "name": "OUT_DIRECTORY",
                    "value": "directory-to-unzip-to",
                },
                {
                    "name": "TASK_ID",
                    "value": "name-you-want-the-task-to-have",
                },
            ]
        }
    ]
}

execute_unzip_job_task = CloudRunExecuteJobOperator(
    task_id=execute_unzip_job_task_id,
    project_id=PROJECT_ID,
    region=REGION,
    job_name=UNZIP_JOB,
    overrides=overrides,
    retries=1,
    retry_delay=timedelta(seconds=60),
    retry_exponential_backoff=True,
    deferrable=DEFERRABLE,
)
```

## Environment variables

- MOUNT_PATH (optional): Absolute path to the mounted writable directory inside the container. Default is `/gcs`.
- IN_FILE (required): Relative path (under MOUNT_PATH) to the ZIP file to decompress.
- OUT_DIRECTORY (required): GCS object prefix (directory‑like path) to upload extracted files under.
- BUCKET (required): Target GCS bucket name.
- TASK_ID (required): Task identifier used for structured logging context.
- DAG_ID (required): DAG/flow identifier used for structured logging context.
- DELETE_ZIP_FILE (optional): When "true", empties the local directory that contains the ZIP file after a successful upload.

## Behavior

- Validates required environment variables at startup; exits non‑zero on missing values.
- Opens `MOUNT_PATH/IN_FILE` as a ZIP archive and iterates entries.
- Skips directory entries; uploads file entries to GCS at `OUT_DIRECTORY/<zip relative path>`.
- Enforces safe path constraints to prevent writing outside `OUT_DIRECTORY` in GCS.
- Closes all readers/writers and reports errors with structured logs.
- If `DELETE_ZIP_FILE=true`, empties the local directory containing the ZIP (removes all entries).
- Exits with code 0 on success.
