// cloudRunUnzipInGcs: extracts a ZIP from a mounted path and streams entries
// directly to GCS under the provided output directory prefix.
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
	// Initialize logger for structured logging to stdout (Cloud Run captures this)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

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

	// Re-instantiate logger with task/dag context
	// This ensures all subsequent log entries include task and DAG identifiers for traceability
	logger = slog.New(slog.NewTextHandler(os.Stdout, nil)).With(
		"task-id", taskID,
		"dag-id", dagID,
	)

	// MOUNT_PATH: Base directory path where the ZIP file is mounted (e.g., Cloud Run volume mount)
	mountPath := os.Getenv("MOUNT_PATH")
	if mountPath == "" {
		mountPath = "/gcs"
		logger.Info("Mount path is not set, using default /gcs")
	}

	// IN_FILE: Filename (not full path) of the ZIP file relative to MOUNT_PATH
	inFile := os.Getenv("IN_FILE")
	if inFile == "" {
		logger.Error("IN_FILE env var is required")
		os.Exit(1)
	}

	// OUT_DIRECTORY: GCS object path prefix (directory) where extracted files will be uploaded
	// All files from the ZIP will be placed under this prefix, preserving their relative paths
	outDirectory := os.Getenv("OUT_DIRECTORY")
	if outDirectory == "" {
		logger.Error("OUT_DIRECTORY env var is required")
		os.Exit(1)
	}

	// BUCKET: GCS bucket name where extracted files will be uploaded
	bucketName := os.Getenv("BUCKET")
	if bucketName == "" {
		logger.Error("BUCKET env var is required")
		os.Exit(1)
	}

	// DELETE_ZIP_FILE: When enabled, removes all files in the directory containing the ZIP file
	// This is useful for cleanup in Cloud Run environments where files are mounted temporarily
	deleteZip := strings.EqualFold(os.Getenv("DELETE_ZIP_FILE"), "true")

	// Construct the full path to the ZIP file by combining mount path and filename
	zipFilePath := fmt.Sprintf("%s/%s", mountPath, inFile)

	// Open the ZIP file for reading
	// zip.OpenReader opens the ZIP archive and reads its central directory
	// This allows random access to individual files within the ZIP without decompressing everything
	r, err := zip.OpenReader(zipFilePath)
	if err != nil {
		logger.Error("Failed to open zip file", "error", err)
		os.Exit(1)
	}
	// Ensure the ZIP reader is closed when the function exits to free resources
	defer r.Close()

	// Initialize GCS client for uploading extracted files
	ctx := context.Background()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		logger.Error("Failed to create storage client", "error", err)
		os.Exit(1)
	}
	defer storageClient.Close()

	// Iterate through all files in the ZIP archive
	// r.File is a slice of *zip.File representing each entry in the ZIP
	for _, f := range r.File {
		// Construct the GCS object path by joining the output directory prefix with the file's name
		// This preserves the directory structure from the ZIP file
		// Example: outDirectory="extracted/", f.Name="data/file.csv" -> "extracted/data/file.csv"
		objectPath := path.Join(outDirectory, f.Name)

		// Security check: prevent path traversal attacks (ZIP slip vulnerability)
		// Malicious ZIP files could contain paths like "../../etc/passwd" to write outside the intended directory
		// We normalize both paths by prefixing with "/" and cleaning them, then check that the object path
		// is within the safe prefix. This prevents any relative path tricks.
		cleaned := path.Clean("/" + objectPath)
		safePrefix := path.Clean("/" + outDirectory)
		// The cleaned path must either:
		// 1. Start with the safe prefix (file is within the output directory), OR
		// 2. Equal the safe prefix (file is at the root of the output directory)
		if !strings.HasPrefix(cleaned, safePrefix) && cleaned != safePrefix {
			logger.Error("illegal file path", "file path", objectPath)
			os.Exit(1)
		}

		// Skip directory entries (ZIP files can contain directory entries)
		// We only process actual files, not directory placeholders
		if f.FileInfo().IsDir() {
			continue
		}

		// Open the file entry within the ZIP for reading
		// This returns a ReadCloser that decompresses the file data on-the-fly
		// The file is not fully decompressed into memory; data is streamed as needed
		zipEntryReader, err := f.Open()
		if err != nil {
			logger.Error("Failed to open file", "error", err)
			os.Exit(1)
		}

		// Create a GCS writer for the destination object
		// The writer buffers data and uploads to GCS when closed or flushed
		writer := storageClient.Bucket(bucketName).Object(objectPath).NewWriter(ctx)

		// Stream the decompressed file contents directly from ZIP to GCS
		// io.Copy efficiently copies data in chunks, avoiding loading entire files into memory
		// This is crucial for large ZIP files with many entries or large individual files
		// The first return value (bytes copied) is ignored since we don't need it
		_, err = io.Copy(writer, zipEntryReader)
		if err != nil {
			logger.Error("Failed to copy contents to GCS", "error", err, "object", objectPath)
			os.Exit(1)
		}

		// Close the GCS writer to finalize the upload
		// Closing the writer triggers the actual upload to GCS and commits the object
		err = writer.Close()
		if err != nil {
			logger.Error("Failed to close GCS writer", "error", err)
			os.Exit(1)
		}

		// Close the ZIP entry reader to free resources
		// This is important for memory management when processing many files
		err = zipEntryReader.Close()
		if err != nil {
			logger.Error("Failed to close zip entry reader", "error", err)
			os.Exit(1)
		}

	}

	// Optional cleanup: remove all files in the directory containing the ZIP file
	// This is useful for cleanup in Cloud Run environments where files are mounted temporarily
	// and should be removed after processing to free up space
	if deleteZip {
		// Instead of removing the directory, ensure the containing directory is empty
		// Extract the directory path from the ZIP file path
		// Example: zipFilePath="/mnt/data/file.zip" -> dirPath="/mnt/data"
		dirPath := path.Dir(zipFilePath)
		// Read all entries in the directory
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			// Ignore "directory does not exist" errors (it may have been deleted already)
			if !os.IsNotExist(err) {
				logger.Error("Failed to read directory contents", "error", err, "dir", dirPath)
				os.Exit(1)
			}
		} else {
			// Remove each entry in the directory (files and subdirectories)
			// This empties the directory but doesn't remove the directory itself
			for _, entry := range entries {
				entryPath := path.Join(dirPath, entry.Name())
				// RemoveAll removes files and directories recursively
				if remErr := os.RemoveAll(entryPath); remErr != nil {
					logger.Error("Failed to remove directory entry", "error", remErr, "path", entryPath)
					os.Exit(1)
				}
			}
			logger.Info("Emptied directory", "dir", dirPath)
		}
	}

	// Log successful completion with bucket and output prefix information
	logger.Info("Unzipped and uploaded successfully", "bucket", bucketName, "prefix", outDirectory)
}
