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

// schemaTransform represents a single column rule provided via SCHEMA_TRANSFORM
type schemaTransform struct {
	ExpectedColumnName string `json:"expected-column-name"`
	Type               string `json:"type"`
	RenameTo           string `json:"rename-to"`
}

// normalizeBqType maps common synonyms to BigQuery primitive types we support here
func normalizeBqType(t string) string {
	tt := strings.ToUpper(strings.TrimSpace(t))
	switch tt {
	case "INT", "INTEGER":
		return "INTEGER"
	case "FLOAT", "DOUBLE", "NUMBER", "NUMERIC", "DECIMAL":
		return "FLOAT"
	case "BOOL", "BOOLEAN":
		return "BOOLEAN"
	case "STRING", "TEXT":
		return "STRING"
	default:
		return tt
	}
}

// valueConformsToType validates a non-empty trimmed value against a normalized BQ type
func valueConformsToType(val string, typeName string) bool {
	switch normalizeBqType(typeName) {
	case "INTEGER":
		return isInteger(val)
	case "FLOAT":
		return isFloat(val)
	case "BOOLEAN":
		return isBoolean(val)
	case "STRING":
		return true
	default:
		// Unknown types treated as STRING to avoid unexpected failures
		return true
	}
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

	// When true, export a small CSV with just cleaned headers and first 5 rows
	exportHead := strings.EqualFold(os.Getenv("EXPORT_HEAD"), "true")

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

	// Optional head export writer (header + first 5 rows)
	var headCsvWriter *csv.Writer
	var headGcsWriter *storage.Writer
	if exportHead {
		headObjectPath := outObjectPath + ".head.csv"
		headGcsWriter = storageClient.Bucket(bucketName).Object(headObjectPath).NewWriter(ctx)
		headGcsWriter.ContentType = "text/csv"
		defer func() {
			if headGcsWriter != nil {
				if err := headGcsWriter.Close(); err != nil {
					logger.Error("Failed to close head GCS writer", "error", err)
					os.Exit(1)
				}
			}
		}()
		headCsvWriter = csv.NewWriter(headGcsWriter)
		logger.Info("EXPORT_HEAD enabled; writing head CSV", "object", headObjectPath)
	}

	originalColumnNames, err := inFileReader.Read()
	if err != nil {
		logger.Error("Failed to read infile", "error", err)
		os.Exit(1)
	}

	// Optional schema transform
	schemaTransformEnv := os.Getenv("SCHEMA_TRANSFORM")
	useTransform := strings.TrimSpace(schemaTransformEnv) != ""
	columnNames := make([]string, len(originalColumnNames))
	expectedTypes := make([]string, len(originalColumnNames))

	if useTransform {
		var transforms []schemaTransform
		if err := json.Unmarshal([]byte(schemaTransformEnv), &transforms); err != nil {
			logger.Error("Failed to parse SCHEMA_TRANSFORM JSON", "error", err)
			os.Exit(1)
		}

		// Build lookup from expected-column-name -> transform
		transformByExpected := make(map[string]schemaTransform, len(transforms))
		for _, tr := range transforms {
			key := strings.TrimSpace(tr.ExpectedColumnName)
			if key == "" {
				logger.Error("SCHEMA_TRANSFORM entry missing expected-column-name")
				os.Exit(1)
			}
			transformByExpected[key] = tr
		}

		// Validate/construct output header and expected types in the CSV header order
		for i, hdr := range originalColumnNames {
			tr, ok := transformByExpected[hdr]
			if !ok {
				logger.Error("CSV header column not found in SCHEMA_TRANSFORM", "column", hdr)
				os.Exit(1)
			}
			newName := tr.RenameTo
			if strings.TrimSpace(newName) == "" {
				newName = hdr
			}
			// If MAKE_COLUMN_NAMES_SQL_FRIENDLY is set, apply it to the target names
			if makeColumnNamesSqlFriendly {
				newName = makeStringSqlFriendly(newName)
			}
			columnNames[i] = newName
			expectedTypes[i] = normalizeBqType(tr.Type)
		}
		logger.Info("Using SCHEMA_TRANSFORM for columns", "columnNames", columnNames)
	} else {
		// No transform: optionally make SQL friendly names
		copy(columnNames, originalColumnNames)
		if makeColumnNamesSqlFriendly {
			for i := range originalColumnNames {
				columnNames[i] = makeStringSqlFriendly(originalColumnNames[i])
			}
		}
		logger.Info("Column names", "columnNames", columnNames)
	}

	outFileWriter.Write(columnNames)
	if exportHead {
		headCsvWriter.Write(columnNames)
		headCsvWriter.Flush()
		if err := headCsvWriter.Error(); err != nil {
			logger.Error("Failed to write header to head CSV", "error", err)
			os.Exit(1)
		}
	}
	outFileWriter.Flush()

	err = outFileWriter.Error()
	if err != nil {
		logger.Error("Failed to flush outfile writer", "error", err)
		os.Exit(1)
	}

	removeValuesBool := len(removeValuesMap) > 0

	// Track possible types only when no schema transform provided
	// We evaluate against primitive candidates (no date/time)
	type candidate struct {
		isInt   bool
		isFloat bool
		isBool  bool
	}

	colCandidates := make([]candidate, len(columnNames))
	if !useTransform {
		for i := range colCandidates {
			colCandidates[i] = candidate{
				isInt:   true,
				isFloat: true,
				isBool:  true,
			}
		}
	}

	lineNumber := 0

	headRowsWritten := 0
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

		// Remove values and update type candidates / enforce transform in a single pass
		for i, v := range rec {
			if removeValuesBool {
				if removeValuesMap[v] {
					rec[i] = ""
					continue
				}
			}
			val := strings.TrimSpace(v)
			if val == "" {
				// empty values are always allowed
				continue
			}
			if useTransform {
				if !valueConformsToType(val, expectedTypes[i]) {
					logger.Error("Type validation failed", "line", lineNumber, "column", columnNames[i], "expectedType", expectedTypes[i], "value", val)
					os.Exit(1)
				}
			} else {
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
		}

		outFileWriter.Write(rec)
		if exportHead && headRowsWritten < 5 {
			headCsvWriter.Write(rec)
			headRowsWritten++
		}
	}

	outFileWriter.Flush()

	err = outFileWriter.Error()
	if err != nil {
		logger.Error("Failed to flush outfile writer", "error", err)
		os.Exit(1)
	}

	if exportHead {
		headCsvWriter.Flush()
		if err := headCsvWriter.Error(); err != nil {
			logger.Error("Failed to flush head CSV writer", "error", err)
			os.Exit(1)
		}
	}

	// Determine final BigQuery types
	finalTypes := make([]string, len(columnNames))
	if useTransform {
		copy(finalTypes, expectedTypes)
	} else {
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
