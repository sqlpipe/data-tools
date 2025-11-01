package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"cloud.google.com/go/storage"
)

type bqField struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Mode string `json:"mode"`
}

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

	inFileName := os.Getenv("IN_FILE")
	if inFileName == "" {
		logger.Error("IN_FILE env var is required")
		os.Exit(1)
	}

	outObjectPath := os.Getenv("OUT_FILE")
	if outObjectPath == "" {
		logger.Error("OUT_FILE env var is required")
		os.Exit(1)
	}

	schemaObjectPath := os.Getenv("SCHEMA_FILE")
	if schemaObjectPath == "" {
		logger.Error("SCHEMA_FILE env var is required")
		os.Exit(1)
	}

	bucketName := os.Getenv("BUCKET")
	if bucketName == "" {
		logger.Error("BUCKET env var is required")
		os.Exit(1)
	}

	deleteInFile := strings.EqualFold(os.Getenv("DELETE_IN_FILE"), "true")

	makeColumnNamesSqlFriendly := strings.EqualFold(os.Getenv("MAKE_COLUMN_NAMES_SQL_FRIENDLY"), "true")

	removeValuesEnv := os.Getenv("REMOVE_VALUES")
	removeValues := strings.Split(removeValuesEnv, ",")

	removeValuesMap := make(map[string]bool)
	for _, value := range removeValues {
		if value == "" {
			continue // ignore empty tokens to avoid treating all empty cells as matches
		}
		removeValuesMap[value] = true
	}

	logger.Info("Remove values", "removeValues", removeValues)

	infilePath := fmt.Sprintf("%s/%s", mountPath, inFileName)

	inFile, err := os.Open(infilePath)
	if err != nil {
		logger.Error("Failed to open infile", "error", err)
		os.Exit(1)
	}
	defer inFile.Close()

	inFileReader := csv.NewReader(inFile)

	// Initialize GCS client and writer
	ctx := context.Background()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		logger.Error("Failed to create storage client", "error", err)
		os.Exit(1)
	}
	defer storageClient.Close()

	gcsWriter := storageClient.Bucket(bucketName).Object(outObjectPath).NewWriter(ctx)
	gcsWriter.ContentType = "text/csv"
	defer func() {
		if err := gcsWriter.Close(); err != nil {
			logger.Error("Failed to close GCS writer", "error", err)
			os.Exit(1)
		}
	}()

	outFileWriter := csv.NewWriter(gcsWriter)

	originalColumnNames, err := inFileReader.Read()
	if err != nil {
		logger.Error("Failed to read infile", "error", err)
		os.Exit(1)
	}

	columnNames := originalColumnNames

	if makeColumnNamesSqlFriendly {
		for i := range originalColumnNames {
			columnNames[i] = makeStringSqlFriendly(originalColumnNames[i])
		}
	}

	logger.Info("Column names", "columnNames", columnNames)

	outFileWriter.Write(columnNames)
	outFileWriter.Flush()

	err = outFileWriter.Error()
	if err != nil {
		logger.Error("Failed to flush outfile writer", "error", err)
		os.Exit(1)
	}

	removeValuesBool := len(removeValuesMap) > 0

	// Track possible BigQuery types per column while reading
	// We evaluate against these primitive candidates (no date/time)
	type candidate struct {
		isInt   bool
		isFloat bool
		isBool  bool
	}

	colCandidates := make([]candidate, len(columnNames))
	for i := range colCandidates {
		colCandidates[i] = candidate{
			isInt:   true,
			isFloat: true,
			isBool:  true,
		}
	}

	lineNumber := 0

	for {

		if lineNumber%100000 == 0 {
			logger.Info("Reading CSV row", "line", lineNumber)
		}

		lineNumber++

		rec, err := inFileReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Error("Failed reading CSV record", "error", err)
			os.Exit(1)
		}

		// Remove values and update type candidates in a single pass
		for i, v := range rec {
			if removeValuesBool {
				if removeValuesMap[v] {
					rec[i] = ""
					continue
				}
			}
			val := strings.TrimSpace(v)
			if val == "" {
				// empty values do not eliminate any candidates; continue
				continue
			}
			c := colCandidates[i]
			if c.isInt && !isInteger(val) {
				c.isInt = false
			}
			if c.isFloat && !isFloat(val) {
				c.isFloat = false
			}
			if c.isBool && !isBoolean(val) {
				c.isBool = false
			}
			colCandidates[i] = c
		}

		outFileWriter.Write(rec)
	}

	outFileWriter.Flush()

	err = outFileWriter.Error()
	if err != nil {
		logger.Error("Failed to flush outfile writer", "error", err)
		os.Exit(1)
	}

	// Determine final BigQuery types with a reasonable precedence (no date/time)
	finalTypes := make([]string, len(columnNames))
	for i, c := range colCandidates {
		// Prefer numeric, then bool, else string
		switch {
		case c.isInt:
			finalTypes[i] = "INTEGER"
		case c.isFloat:
			finalTypes[i] = "FLOAT"
		case c.isBool:
			finalTypes[i] = "BOOLEAN"
		default:
			finalTypes[i] = "STRING"
		}
	}

	// Build BigQuery schema JSON (array of fields)
	schema := make([]bqField, 0, len(columnNames))
	for i, name := range columnNames {
		fieldType := finalTypes[i]
		if fieldType == "" {
			fieldType = "STRING"
		}
		schema = append(schema, bqField{Name: name, Type: fieldType, Mode: "NULLABLE"})
	}

	schemaBytes, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		logger.Error("Failed to marshal schema JSON", "error", err)
		os.Exit(1)
	}

	// Write schema JSON to GCS
	schemaWriter := storageClient.Bucket(bucketName).Object(schemaObjectPath).NewWriter(ctx)
	schemaWriter.ContentType = "application/json"
	if _, err := schemaWriter.Write(schemaBytes); err != nil {
		logger.Error("Failed to write schema to GCS", "error", err)
		_ = schemaWriter.Close()
		os.Exit(1)
	}
	if err := schemaWriter.Close(); err != nil {
		logger.Error("Failed to close schema writer", "error", err)
		os.Exit(1)
	}

	logger.Info("CSV and schema written", "bucket", bucketName, "csvObject", outObjectPath, "schemaObject", schemaObjectPath)

	if deleteInFile {
		err = os.Remove(infilePath)
		if err != nil && !os.IsNotExist(err) {
			logger.Error("Failed to delete infile", "error", err)
			os.Exit(1)
		}
		logger.Info("Deleted infile", "file", infilePath)
	}
}
