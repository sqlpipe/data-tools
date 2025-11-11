# cloudRunDownloadToGcs

Downloads a file from a source URL to a path on a mounted filesystem. Designed to be used within GCP Cloud Run Jobs, and triggered by Airflow.

## Entrypoint

- `cmd/cloudRunDownloadToGcs/main.go`

## Environment variables

- MOUNT_PATH (required): Absolute path to the mounted writable directory inside the container.
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

## Example (docker)

```bash
docker run --rm \
  -e TASK_ID=task-123 \
  -e DAG_ID=dag-abc \
  -e MOUNT_PATH=/data \
  -e LANDING_FILE=incoming/myfile.csv \
  -e SOURCE_URL=https://example.com/myfile.csv \
  -v "$(pwd)/data:/data" \
  sqlpipe/cloud-run-download-to-gcs:dev-<tag>
```

## Notes

- The tool does not upload to GCS; it only writes to the mounted filesystem. Use other tools in this repository to process or move the file afterward.
