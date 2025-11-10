package main

import (
	"bufio"
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

	exportNilTransform := strings.EqualFold(os.Getenv("EXPORT_NIL_TRANSFORM"), "true")
	if exportNilTransform {
		logger.Info("EXPORT_NIL_TRANSFORM enabled; exporting nil transform")
	}

	deleteInFile := strings.EqualFold(os.Getenv("DELETE_IN_FILE"), "true")
	if deleteInFile {
		logger.Info("DELETE_IN_FILE enabled; deleting infile")
	}

	makeColumnNamesSqlFriendly := strings.EqualFold(os.Getenv("MAKE_COLUMN_NAMES_SQL_FRIENDLY"), "true")
	if makeColumnNamesSqlFriendly {
		logger.Info("MAKE_COLUMN_NAMES_SQL_FRIENDLY enabled; making column names SQL friendly")
	}

	// When true, export a small CSV with just cleaned headers and first 5 rows
	exportHead := strings.EqualFold(os.Getenv("EXPORT_HEAD"), "true")
	if exportHead {
		logger.Info("EXPORT_HEAD enabled; exporting head")
	}

	removeValuesEnv := os.Getenv("REMOVE_VALUES")
	if removeValuesEnv != "" {
		logger.Info("REMOVE_VALUES env var is set; removing values", "remove-values", removeValuesEnv)
	}
	removeValues := strings.Split(removeValuesEnv, ",")

	removeValuesMap := make(map[string]bool)
	for _, value := range removeValues {
		if value == "" {
			continue // ignore empty tokens to avoid treating all empty cells as matches
		}
		removeValuesMap[value] = true
	}

	infilePath := fmt.Sprintf("%s/%s", mountPath, inFileName)

	inFile, err := os.Open(infilePath)
	if err != nil {
		logger.Error("Failed to open infile", "error", err)
		os.Exit(1)
	}
	defer inFile.Close()

	// We'll read the first line as a raw string when SCHEMA_TRANSFORM is set
	// to allow substring replacements that may include commas.
	// After header handling, we will construct a csv.Reader for the remaining rows.
	bufReader := bufio.NewReader(inFile)
	rawHeaderLine, err := bufReader.ReadString('\n')
	if err != nil && err != io.EOF { // allow EOF for single-line files
		logger.Error("Failed to read infile header line", "error", err)
		os.Exit(1)
	}
	// Trim trailing newlines (support \r\n and \n)
	rawHeaderLine = strings.TrimRight(rawHeaderLine, "\r\n")

	// Optional schema transform
	schemaTransformEnv := os.Getenv("SCHEMA_TRANSFORM")
	useTransform := strings.TrimSpace(schemaTransformEnv) != ""
	var originalColumnNames []string
	var columnNames []string
	var expectedTypes []string

	if useTransform {
		var transforms []schemaTransform
		if err := json.Unmarshal([]byte(schemaTransformEnv), &transforms); err != nil {
			logger.Error("Failed to parse SCHEMA_TRANSFORM JSON", "error", err)
			os.Exit(1)
		}

		// Perform substring replacements on the raw header line using the transforms.
		// Replacement is exact, case-sensitive, and uses rename-to if provided, else expected name.
		replacedHeader := rawHeaderLine
		for _, tr := range transforms {
			expected := strings.TrimSpace(tr.ExpectedColumnName)
			if expected == "" {
				logger.Error("SCHEMA_TRANSFORM entry missing expected-column-name")
				os.Exit(1)
			}
			target := strings.TrimSpace(tr.RenameTo)
			if target == "" {
				target = expected
			}
			replacedHeader = strings.ReplaceAll(replacedHeader, expected, target)
		}

		// Parse the replaced header using CSV rules to get final header tokens
		headerReader := csv.NewReader(strings.NewReader(replacedHeader))
		parsedHeader, err := headerReader.Read()
		if err != nil {
			logger.Error("Failed to parse transformed header as CSV", "error", err)
			os.Exit(1)
		}
		originalColumnNames = parsedHeader

		// Validate exact match in number and names (in order) against transforms
		if len(parsedHeader) != len(transforms) {
			logger.Error("Header column count does not match SCHEMA_TRANSFORM", "headerCount", len(parsedHeader), "transformCount", len(transforms))
			os.Exit(1)
		}

		columnNames = make([]string, len(parsedHeader))
		expectedTypes = make([]string, len(parsedHeader))
		for i, hdr := range parsedHeader {
			desiredName := strings.TrimSpace(transforms[i].RenameTo)
			if desiredName == "" {
				desiredName = strings.TrimSpace(transforms[i].ExpectedColumnName)
			}
			if !strings.EqualFold(hdr, desiredName) {
				logger.Error("Header column does not match SCHEMA_TRANSFORM at position", "index", i, "header", hdr, "expected", desiredName)
				os.Exit(1)
			}
			columnNames[i] = desiredName
			expectedTypes[i] = normalizeBqType(transforms[i].Type)
		}
		logger.Info("Using SCHEMA_TRANSFORM for columns", "column-names", columnNames)
	} else {
		// No transform: parse header normally then optionally make SQL friendly names
		headerReader := csv.NewReader(strings.NewReader(rawHeaderLine))
		parsedHeader, err := headerReader.Read()
		if err != nil {
			logger.Error("Failed to parse header as CSV", "error", err)
			os.Exit(1)
		}
		originalColumnNames = parsedHeader
		columnNames = make([]string, len(parsedHeader))
		copy(columnNames, parsedHeader)
		if makeColumnNamesSqlFriendly {
			for i := range columnNames {
				columnNames[i] = makeStringSqlFriendly(columnNames[i])
			}
		}
		logger.Info("Column names", "columnNames", columnNames)
	}

	// Initialize context, GCS client and writers AFTER header/transform is finalized
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
	// Create csv reader for the remaining rows starting after the header line we consumed
	inFileReader := csv.NewReader(bufReader)

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

	// Optionally export a nil transform JSON next to the schema JSON
	if exportNilTransform {
		// Map BQ primitive types to transform types (lowercase)
		mapBqToTransformType := func(bqType string) string {
			switch strings.ToUpper(strings.TrimSpace(bqType)) {
			case "INTEGER", "FLOAT", "NUMERIC", "DECIMAL", "NUMBER", "DOUBLE":
				return "number"
			case "BOOLEAN", "BOOL":
				return "boolean"
			default:
				return "string"
			}
		}

		// Build transforms using original headers as expected names and current columnNames as rename targets
		transforms := make([]schemaTransform, 0, len(columnNames))
		for i := range columnNames {
			transforms = append(transforms, schemaTransform{
				ExpectedColumnName: originalColumnNames[i],
				Type:               mapBqToTransformType(finalTypes[i]),
				RenameTo:           columnNames[i],
			})
		}

		transformBytes, err := json.MarshalIndent(transforms, "", "  ")
		if err != nil {
			logger.Error("Failed to marshal nil transform JSON", "error", err)
			os.Exit(1)
		}

		// Derive destination object path: same directory as schemaObjectPath
		transformObjectPath := func() string {
			idx := strings.LastIndex(schemaObjectPath, "/")
			if idx == -1 {
				return "nil-transform.json"
			}
			return schemaObjectPath[:idx+1] + "nil-transform.json"
		}()

		transformWriter := storageClient.Bucket(bucketName).Object(transformObjectPath).NewWriter(ctx)
		transformWriter.ContentType = "application/json"
		if _, err := transformWriter.Write(transformBytes); err != nil {
			logger.Error("Failed to write nil transform to GCS", "error", err)
			_ = transformWriter.Close()
			os.Exit(1)
		}
		if err := transformWriter.Close(); err != nil {
			logger.Error("Failed to close nil transform writer", "error", err)
			os.Exit(1)
		}

		logger.Info("Nil transform written", "bucket", bucketName, "object", transformObjectPath)
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
