# cloudRunUnzipInGcs

Unzips a local ZIP file from a mounted path and uploads its contents to Google Cloud Storage under a specified object prefix. Performs safe path checks to avoid zip‑slip attacks and can optionally clean up the local directory after completion.

## Entrypoint

- `cmd/cloudRunUnzipInGcs/main.go`

## Environment variables

- MOUNT_PATH (required): Absolute path to the mounted directory where the ZIP file resides.
- IN_FILE (required): Relative path (under MOUNT_PATH) to the ZIP file to decompress.
- OUT_DIRECTORY (required): GCS object prefix (directory‑like path) to upload extracted files under.
- BUCKET (required): Target GCS bucket name.
- TASK_ID (required): Task identifier used for structured logging context.
- DAG_ID (required): DAG/flow identifier used for structured logging context.
- DELETE_ZIP_FILE (optional, default false): When "true", empties the directory that contains the ZIP file after a successful upload.

## Behavior

- Validates required environment variables at startup; exits non‑zero on missing values.
- Opens `MOUNT_PATH/IN_FILE` as a ZIP archive and iterates entries.
- Skips directory entries; uploads file entries to GCS at `OUT_DIRECTORY/<zip relative path>`.
- Enforces safe path constraints to prevent writing outside `OUT_DIRECTORY` in GCS.
- Closes all readers/writers and reports errors with structured logs.
- If `DELETE_ZIP_FILE=true`, empties the local directory containing the ZIP (removes all entries).
- Exits with code 0 on success.

## Example (docker)

```bash
docker run --rm \
  -e TASK_ID=task-123 \
  -e DAG_ID=dag-abc \
  -e MOUNT_PATH=/data \
  -e IN_FILE=landing/archive.zip \
  -e OUT_DIRECTORY=unzipped/2025-11-11 \
  -e BUCKET=my-bucket \
  -e DELETE_ZIP_FILE=true \
  -v "$(pwd)/data:/data" \
  -v "$GOOGLE_APPLICATION_CREDENTIALS:/var/secrets/google/key.json:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/var/secrets/google/key.json \
  sqlpipe/cloud-run-unzip-in-gcs:dev-<tag>
```

## Notes

- Requires application default credentials to access GCS (e.g., via `GOOGLE_APPLICATION_CREDENTIALS`), or a workload identity in managed environments.
- Object paths are created by joining `OUT_DIRECTORY` with each file’s relative path inside the ZIP, preserving the ZIP’s internal structure.
