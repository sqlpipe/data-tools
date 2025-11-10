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

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Read required env vars before logger instantiation
	mountPath := os.Getenv("MOUNT_PATH")
	if mountPath == "" {
		logger.Error("Mount path is required")
		os.Exit(1)
	}

	landingFile := os.Getenv("LANDING_FILE")
	if landingFile == "" {
		logger.Error("Landing file is required")
		os.Exit(1)
	}

	sourceUrl := os.Getenv("SOURCE_URL")
	if sourceUrl == "" {
		logger.Error("Source URL is required")
		os.Exit(1)
	}

	// Required context env vars
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
	logger = slog.New(slog.NewTextHandler(os.Stdout, nil)).With(
		"task-id", taskID,
		"dag-id", dagID,
	)

	downloadTo := fmt.Sprintf("%s/%s", mountPath, landingFile)

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(downloadTo), 0o755); err != nil {
		logger.Error("Failed to create destination directory", "error", err)
		os.Exit(1)
	}

	file, err := os.Create(downloadTo)
	if err != nil {
		logger.Error("Failed to create file", "error", err)
		os.Exit(1)
	}
	defer file.Close()

	logger.Info("File created", "mountPath", mountPath, "landingFile", landingFile, "downloadTo", downloadTo)

	resp, err := http.Get(sourceUrl)
	if err != nil {
		logger.Error("Failed to download file", "error", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Error("Failed to download file", "status", resp.StatusCode)
		os.Exit(1)
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		logger.Error("Failed to copy file", "error", err)
		os.Exit(1)
	}

	logger.Info("File downloaded", "sourceUrl", sourceUrl, "downloadTo", downloadTo)

	os.Exit(0)
}
