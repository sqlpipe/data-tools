package main

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

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

	// Re-instantiate logger with task/dag context
	logger = slog.New(slog.NewTextHandler(os.Stdout, nil)).With(
		"task-id", taskID,
		"dag-id", dagID,
	)

	mountPath := os.Getenv("MOUNT_PATH")
	if mountPath == "" {
		logger.Error("MOUNT_PATH env var is required")
		os.Exit(1)
	}

	inFile := os.Getenv("IN_FILE")
	if inFile == "" {
		logger.Error("IN_FILE env var is required")
		os.Exit(1)
	}

	outDirectory := os.Getenv("OUT_DIRECTORY")
	if outDirectory == "" {
		logger.Error("OUT_DIRECTORY env var is required")
		os.Exit(1)
	}

	bucketName := os.Getenv("BUCKET")
	if bucketName == "" {
		logger.Error("BUCKET env var is required")
		os.Exit(1)
	}

	deleteZip := strings.EqualFold(os.Getenv("DELETE_ZIP_FILE"), "true")

	zipFilePath := fmt.Sprintf("%s/%s", mountPath, inFile)

	r, err := zip.OpenReader(zipFilePath)
	if err != nil {
		logger.Error("Failed to open zip file", "error", err)
		os.Exit(1)
	}
	defer r.Close()

	ctx := context.Background()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		logger.Error("Failed to create storage client", "error", err)
		os.Exit(1)
	}
	defer storageClient.Close()

	for _, f := range r.File {
		objectPath := path.Join(outDirectory, f.Name)

		cleaned := path.Clean("/" + objectPath)
		safePrefix := path.Clean("/" + outDirectory)
		if !strings.HasPrefix(cleaned, safePrefix) && cleaned != safePrefix {
			logger.Error("illegal file path", "file path", objectPath)
			os.Exit(1)
		}

		if f.FileInfo().IsDir() {
			continue
		}

		zipEntryReader, err := f.Open()
		if err != nil {
			logger.Error("Failed to open file", "error", err)
			os.Exit(1)
		}

		writer := storageClient.Bucket(bucketName).Object(objectPath).NewWriter(ctx)

		_, err = io.Copy(writer, zipEntryReader)
		if err != nil {
			logger.Error("Failed to copy contents to GCS", "error", err, "object", objectPath)
			os.Exit(1)
		}

		err = writer.Close()
		if err != nil {
			logger.Error("Failed to close GCS writer", "error", err)
			os.Exit(1)
		}

		err = zipEntryReader.Close()
		if err != nil {
			logger.Error("Failed to close zip entry reader", "error", err)
			os.Exit(1)
		}

	}

	if deleteZip {
		// Instead of removing the directory, ensure the containing directory is empty
		dirPath := path.Dir(zipFilePath)
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			if !os.IsNotExist(err) {
				logger.Error("Failed to read directory contents", "error", err, "dir", dirPath)
				os.Exit(1)
			}
		} else {
			for _, entry := range entries {
				entryPath := path.Join(dirPath, entry.Name())
				if remErr := os.RemoveAll(entryPath); remErr != nil {
					logger.Error("Failed to remove directory entry", "error", remErr, "path", entryPath)
					os.Exit(1)
				}
			}
			logger.Info("Emptied directory", "dir", dirPath)
		}
	}

	logger.Info("Unzipped and uploaded successfully", "bucket", bucketName, "prefix", outDirectory)
}
