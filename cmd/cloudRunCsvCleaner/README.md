## cloudRunCsvCleaner

Cleans and normalizes CSV data from a mounted filesystem, writes the cleaned CSV to Google Cloud Storage, and emits a matching BigQuery schema JSON. Supports optional header transformations, SQL‑friendly header normalization, value removal, type validation or inference, head preview export, and a “nil transform” template.

### Entrypoint

- `cmd/cloudRunCsvCleaner/main.go` (helpers in `cmd/cloudRunCsvCleaner/helpers.go`)

### Environment variables

Required:

- MOUNT_PATH: Absolute path to the mounted directory containing the input CSV.
- IN_FILE: Relative path (under MOUNT_PATH) to the input CSV file.
- OUT_FILE: Destination GCS object path for the cleaned CSV (e.g., `clean/my.csv`).
- SCHEMA_FILE: Destination GCS object path for the BigQuery schema JSON (e.g., `clean/my.schema.json`).
- BUCKET: Target GCS bucket name.
- TASK_ID: Task identifier used for structured logging context.
- DAG_ID: DAG/flow identifier used for structured logging context.

Optional:

- EXPORT_NIL_TRANSFORM: When "true", writes `nil-transform.json` next to `SCHEMA_FILE`, describing a no‑op transform mapping original headers to final headers with inferred/validated types.
- DELETE_IN_FILE: When "true", deletes the local input file after successful processing.
- MAKE_COLUMN_NAMES_SQL_FRIENDLY: When "true", converts header names to SQL‑friendly identifiers (e.g., lowercasing, replacing spaces, etc.).
- EXPORT_HEAD: When "true", writes a small CSV preview alongside the cleaned CSV containing only the header and first 5 data rows. The preview object path is `OUT_FILE + ".head.csv"`.
- REMOVE_VALUES: Comma‑separated list of exact cell values to blank out during processing. Empty tokens are ignored (so not all empty cells are matched).
- SCHEMA_TRANSFORM: JSON array describing an explicit schema and header mapping. When set, the header is transformed and all values are validated against the provided types. Structure per entry:
  - `expected-column-name` (string): The original header token to look for.
  - `rename-to` (string, optional): The desired final column name; defaults to `expected-column-name` if omitted.
  - `type` (string): Expected data type for the column; accepted values are case‑insensitive and normalized to BigQuery primitives. Common values: `INTEGER`, `FLOAT`, `BOOLEAN`, `STRING` (aliases like `NUMBER`, `DECIMAL`, `DOUBLE`, `BOOL` are also handled).

Example `SCHEMA_TRANSFORM` JSON:

```json
[
  { "expected-column-name": "User Id", "rename-to": "user_id", "type": "INTEGER" },
  { "expected-column-name": "Active", "type": "BOOLEAN" },
  { "expected-column-name": "Score", "type": "FLOAT" },
  { "expected-column-name": "Notes", "type": "STRING" }
]
```

### Behavior

- Reads the first line as a raw header string, optionally applies `SCHEMA_TRANSFORM` substring replacements, then parses the resulting header as CSV.
- If `SCHEMA_TRANSFORM` is provided:
  - Validates that the number/order of headers matches the transform definition (after replacement/renaming).
  - Validates each non‑empty cell against the specified column type.
- If `SCHEMA_TRANSFORM` is not provided:
  - Optionally makes column names SQL‑friendly (`MAKE_COLUMN_NAMES_SQL_FRIENDLY`).
  - Infers primitive types per column across all rows (Integer → Float → Boolean → String fallback).
- Applies `REMOVE_VALUES` by replacing exact matches with empty strings before validation/inference.
- Writes cleaned CSV to `gs://BUCKET/OUT_FILE`.
- Builds a BigQuery schema JSON (array of `{ "name", "type", "mode": "NULLABLE" }`) and writes it to `gs://BUCKET/SCHEMA_FILE`.
- If `EXPORT_HEAD` is true, writes a header + 5‑row preview to `gs://BUCKET/<OUT_FILE>.head.csv`.
- If `EXPORT_NIL_TRANSFORM` is true, writes `nil-transform.json` next to the schema file, mapping original headers to final names and mapping BigQuery types back to transform‑style types (`string`, `number`, `boolean`). 
- If `DELETE_IN_FILE` is true, removes the local input file.
- Emits structured logs with `task-id` and `dag-id` and progress lines every 100,000 rows.
- Exits with code 0 on success.

### Example (docker)

```bash
docker run --rm \
  -e TASK_ID=task-123 \
  -e DAG_ID=dag-abc \
  -e MOUNT_PATH=/data \
  -e IN_FILE=landing/input.csv \
  -e OUT_FILE=clean/output.csv \
  -e SCHEMA_FILE=clean/output.schema.json \
  -e BUCKET=my-bucket \
  -e MAKE_COLUMN_NAMES_SQL_FRIENDLY=true \
  -e EXPORT_HEAD=true \
  -e REMOVE_VALUES="N/A,NULL" \
  -v "$(pwd)/data:/data" \
  -v "$GOOGLE_APPLICATION_CREDENTIALS:/var/secrets/google/key.json:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/var/secrets/google/key.json \
  sqlpipe/cloud-run-csv-cleaner:dev-<tag>
```

With an explicit transform:

```bash
docker run --rm \
  -e TASK_ID=task-123 \
  -e DAG_ID=dag-abc \
  -e MOUNT_PATH=/data \
  -e IN_FILE=landing/input.csv \
  -e OUT_FILE=clean/output.csv \
  -e SCHEMA_FILE=clean/output.schema.json \
  -e BUCKET=my-bucket \
  -e SCHEMA_TRANSFORM='[{"expected-column-name":"User Id","rename-to":"user_id","type":"INTEGER"},{"expected-column-name":"Active","type":"BOOLEAN"}]' \
  -v "$(pwd)/data:/data" \
  -v "$GOOGLE_APPLICATION_CREDENTIALS:/var/secrets/google/key.json:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/var/secrets/google/key.json \
  sqlpipe/cloud-run-csv-cleaner:dev-<tag>
```

### Outputs

- Cleaned CSV: `gs://BUCKET/OUT_FILE`
- BigQuery schema JSON: `gs://BUCKET/SCHEMA_FILE`
- Optional head preview: `gs://BUCKET/<OUT_FILE>.head.csv`
- Optional nil transform: `gs://BUCKET/<dir-of-SCHEMA_FILE>/nil-transform.json`

### Notes

- When `SCHEMA_TRANSFORM` is set, type validation is strict; any mismatch results in an immediate non‑zero exit.
- Without `SCHEMA_TRANSFORM`, type inference only considers primitive numeric and boolean categories; all others default to `STRING`.


