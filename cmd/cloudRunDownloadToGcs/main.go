package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	mountPath := os.Getenv("MOUNT_PATH")
	if mountPath == "" {
		logger.Error("Mount path is required")
		os.Exit(1)
	}

	filePath := os.Getenv("FILE_PATH")
	if filePath == "" {
		logger.Error("File path is required")
		os.Exit(1)
	}

	url := os.Getenv("URL")
	if url == "" {
		logger.Error("URL is required")
		os.Exit(1)
	}

	downloadTo := fmt.Sprintf("%s/%s", mountPath, filePath)

	file, err := os.Create(downloadTo)
	if err != nil {
		logger.Error("Failed to create file", "error", err)
		os.Exit(1)
	}
	defer file.Close()

	logger.Info("File created", "mountPath", mountPath, "filePath", filePath, "downloadTo", downloadTo)

	resp, err := http.Get(url)
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

	logger.Info("File downloaded", "url", url, "downloadTo", downloadTo)

	os.Exit(0)
}
