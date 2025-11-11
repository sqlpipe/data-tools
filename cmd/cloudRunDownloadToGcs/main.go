// cloudRunDownloadToGcs: downloads a file from SOURCE_URL to a local mount path
// so subsequent tasks can process or upload it.
package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
)

func main() {
	// Initialize basic logger for structured logging to stdout (Cloud Run captures this)
	// This initial logger is used for error messages before we have task/dag context
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Read required env vars before logger instantiation
	// MOUNT_PATH: Base directory path where files are mounted (e.g., Cloud Run volume mount)
	// This is where the downloaded file will be saved
	mountPath := os.Getenv("MOUNT_PATH")
	if mountPath == "" {
		mountPath = "/gcs"
		logger.Info("Mount path is not set, using default /gcs")
	}

	// LANDING_FILE: Filename (can include subdirectories) where the downloaded file will be saved
	// This is relative to MOUNT_PATH. Examples: "data.csv", "downloads/file.zip"
	landingFile := os.Getenv("LANDING_FILE")
	if landingFile == "" {
		logger.Error("Landing file is required")
		os.Exit(1)
	}

	// SOURCE_URL: HTTP/HTTPS URL of the file to download
	// This can be any publicly accessible URL or a URL accessible to the Cloud Run service
	sourceUrl := os.Getenv("SOURCE_URL")
	if sourceUrl == "" {
		logger.Error("Source URL is required")
		os.Exit(1)
	}

	// Required context env vars
	// TASK_ID and DAG_ID are used for logging context and tracking execution in Airflow/Cloud Composer
	taskID := os.Getenv("TASK_ID")
	if taskID == "" {
		logger.Error("TASK_ID is required")
		os.Exit(1)
	}

	dagID := os.Getenv("DAG_ID")
	if dagID == "" {
		logger.Error("DAG_ID is required")
		os.Exit(1)
	}

	// Instantiate new logger once we have all the env vars
	// This ensures all subsequent log entries include task and DAG identifiers for traceability
	logger = slog.New(slog.NewTextHandler(os.Stdout, nil)).With(
		"task-id", taskID,
		"dag-id", dagID,
	)

	// Construct the full destination path by combining mount path and landing file
	// Example: mountPath="/mnt/data", landingFile="downloads/file.csv" -> "/mnt/data/downloads/file.csv"
	downloadTo := fmt.Sprintf("%s/%s", mountPath, landingFile)

	// Ensure destination directory exists
	// filepath.Dir extracts the directory portion of the path (e.g., "/mnt/data/downloads" from "/mnt/data/downloads/file.csv")
	// MkdirAll creates all necessary parent directories with permissions 0o755 (rwxr-xr-x)
	// This handles cases where LANDING_FILE includes subdirectories that don't exist yet
	if err := os.MkdirAll(filepath.Dir(downloadTo), 0o755); err != nil {
		logger.Error("Failed to create destination directory", "error", err)
		os.Exit(1)
	}

	// Create the destination file for writing
	// os.Create truncates the file if it exists (overwrites), or creates a new empty file
	// This ensures we start with a clean file for the download
	file, err := os.Create(downloadTo)
	if err != nil {
		logger.Error("Failed to create file", "error", err)
		os.Exit(1)
	}
	// Ensure the file is closed when the function exits (even on error)
	defer file.Close()

	logger.Info("File created", "mountPath", mountPath, "landingFile", landingFile, "downloadTo", downloadTo)

	// Perform HTTP GET request to download the file from the source URL
	// http.Get follows redirects automatically (up to 10 redirects by default)
	// This handles HTTP and HTTPS URLs transparently
	resp, err := http.Get(sourceUrl)
	if err != nil {
		logger.Error("Failed to download file", "error", err)
		os.Exit(1)
	}
	// Ensure the response body is closed when the function exits
	// This is important to free network resources and prevent connection leaks
	defer resp.Body.Close()

	// Check HTTP status code to ensure the request was successful
	// Only HTTP 200 (StatusOK) is considered successful
	// Other status codes (404, 500, etc.) indicate the download failed
	if resp.StatusCode != http.StatusOK {
		logger.Error("Failed to download file", "status", resp.StatusCode)
		os.Exit(1)
	}

	// Copy the response body (file contents) to the destination file
	// io.Copy efficiently streams data from the HTTP response to the file
	// It handles large files by copying in chunks, avoiding loading everything into memory
	// The first return value is the number of bytes copied (we ignore it here)
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		logger.Error("Failed to copy file", "error", err)
		os.Exit(1)
	}

	logger.Info("File downloaded", "sourceUrl", sourceUrl, "downloadTo", downloadTo)

	// Explicitly exit with success code (though this is the default behavior)
	// This makes the success condition clear and explicit
	os.Exit(0)
}
