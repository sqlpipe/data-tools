# cloudRunDownloadToGcs

Downloads a file from a source URL to a path on a mounted filesystem. Designed to be used within GCP Cloud Run Jobs, and triggered by Airflow.

## Creating a job

I recommend creating a job in Airflow, instead of the console. It's not too hard. Here's an example pattern that checks if a download job exists inside Cloud Run. If it does, it updates it to use the latest job spec. If not, it creates it:

```
download_job_spec = {
    "labels": {
        "optional-labels-to-track": "how-much-stuff-costs"
    },
    "template": {
        "template": {
            "containers": [
                {
                    "image": sqlpipe/cloud-run-csv-cleaner:3,
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
                            "name": "DAG_ID",
                            "value": {{ dag.dag_id }},
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

def branch_on_download_job_existence(**_):
    hook = CloudRunHook(gcp_conn_id="google_cloud_default")
    try:
        hook.get_job(job_name=DOWNLOAD_JOB, region=REGION, project_id=PROJECT_ID)
        return "update-download-job-task"
    except NotFound:
        return "create-download-job-task"

branch_on_download_job_existence_task = BranchPythonOperator(
    task_id="branch-on-download-job-existence-task",
    python_callable=branch_on_download_job_existence,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)

update_download_job_task = CloudRunUpdateJobOperator(
    task_id="update-download-job-task",
    project_id=PROJECT_ID,
    region=REGION,
    job_name=DOWNLOAD_JOB,
    job=download_job_spec,
)

create_download_job_task = CloudRunCreateJobOperator(
    task_id="create-download-job-task",
    project_id=PROJECT_ID,
    region=REGION,
    job_name=DOWNLOAD_JOB,
    job=download_job_spec,
)

overrides = {
    "container_overrides": [
        {
            "env": [
                {
                    "name": "LANDING_FILE",
                    "value": "where-to-put-the-file-in-gcs",
                },
                {
                    "name": "SOURCE_URL",
                    "value": "url-to-download.com",
                },
                {
                    "name": "TASK_ID",
                    "value": "name-you-want-the-task-to-have",
                },
            ]
        }
    ]
}

execute_download_job_task = CloudRunExecuteJobOperator(
    task_id=execute_download_job_task_id,
    project_id=PROJECT_ID,
    region=REGION,
    job_name=DOWNLOAD_JOB,
    overrides=overrides,
)
```

## Environment variables

- MOUNT_PATH (optional): Absolute path to the mounted writable directory inside the container. Default is `/gcs`.
- LANDING_FILE (required): Relative file path (under MOUNT_PATH) where the downloaded file will be written.
- SOURCE_URL (required): HTTP(S) URL to download.
- TASK_ID (required): Task identifier used for structured logging context.
- DAG_ID (required): DAG/flow identifier used for structured logging context.

## Behavior

- Validates required environment variables at startup; exits non‑zero on missing values.
- Ensures the destination directory exists (creates intermediate directories).
- Performs an HTTP GET to `SOURCE_URL` and requires HTTP 200 OK.
- Streams response content to `MOUNT_PATH/LANDING_FILE`.
- Emits structured logs with `task-id` and `dag-id`.
- Exits with code 0 on success.
