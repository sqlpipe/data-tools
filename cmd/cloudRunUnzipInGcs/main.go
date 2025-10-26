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

	outDir := os.Getenv("OUT_DIR")
	if outDir == "" {
		logger.Error("OUT_DIR env var is required")
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
		objectPath := path.Join(outDir, f.Name)

		cleaned := path.Clean("/" + objectPath)
		safePrefix := path.Clean("/" + outDir)
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
		if err := os.Remove(zipFilePath); err != nil {
			logger.Error("Failed to delete zip file", "error", err, "file", zipFilePath)
		} else {
			logger.Info("Deleted zip file", "file", zipFilePath)
		}
	}

	logger.Info("Unzipped and uploaded successfully", "bucket", bucketName, "prefix", outDir)
}
