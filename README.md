# SQLpipe Data Tools

A small collection of free, single‑purpose data movement utilities intended to be run in containerized environments. Each tool focuses on one task and uses simple environment‑based configuration. The repository also includes a `Makefile` with convenient targets for building and pushing images, plus basic maintenance tasks.

## What’s in this repository

- **Top‑level tooling**: See the `Makefile` for build/push targets and maintenance recipes.
- **Individual tools** (under `cmd/`) are independent binaries with their own `Dockerfile`s.

## Available tools

| Tool (binary) | Purpose | Code location |
| --- | --- | --- |
| cloudRunDownloadToGcs | Downloads a file from a source URL to a mounted filesystem path. Minimal logging and strict env var validation; creates destination directories as needed. | `cmd/cloudRunDownloadToGcs/` (entrypoint: `cmd/cloudRunDownloadToGcs/main.go`) |
| cloudRunUnzipInGcs | Extracts a ZIP from a mounted path and streams files directly to Google Cloud Storage under a given prefix. Includes safe path checks and an option to clean up the extracted local directory. | `cmd/cloudRunUnzipInGcs/` (entrypoint: `cmd/cloudRunUnzipInGcs/main.go`) |
| cloudRunCsvCleaner | Cleans and normalizes CSVs read from a mounted path, optionally renames headers, makes names SQL‑friendly, removes specified values, validates or infers column types, writes the cleaned CSV to GCS, and emits a matching BigQuery schema JSON (with optional head export and "nil transform" JSON). | `cmd/cloudRunCsvCleaner/` (entrypoint: `cmd/cloudRunCsvCleaner/main.go`) |

## Docker Hub

All tools are available on Docker Hub under the `sqlpipe` organization:

- **cloudRunDownloadToGcs**: [`sqlpipe/cloud-run-download-to-gcs`](https://hub.docker.com/r/sqlpipe/cloud-run-download-to-gcs)
- **cloudRunUnzipInGcs**: [`sqlpipe/cloud-run-unzip-in-gcs`](https://hub.docker.com/r/sqlpipe/cloud-run-unzip-in-gcs)
- **cloudRunCsvCleaner**: [`sqlpipe/cloud-run-csv-cleaner`](https://hub.docker.com/r/sqlpipe/cloud-run-csv-cleaner)

Production images are tagged with version numbers (e.g., `:2`), while development builds use timestamp-based tags (e.g., `:dev-1234567890`).

## Makefile overview

The top‑level `Makefile` provides:

- Build and push targets for each tool (dev and prod image tags).
- Project hygiene tasks: dependency tidy/vendor, verification, vet/staticcheck, and tests.

Relevant targets (names only, see `Makefile` for details):

- docker/push/cloudRunDownloadToGcs/dev, docker/push/cloudRunDownloadToGcs/prod
- docker/push/cloudRunUnzipInGcs/dev, docker/push/cloudRunUnzipInGcs/prod
- docker/push/cloudRunCsvCleaner/dev, docker/push/cloudRunCsvCleaner/prod
- tidy, audit

## Next steps

Separate READMEs for each tool in `cmd/` will document their specific behavior and configuration in more detail.
