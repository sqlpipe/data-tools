package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"unicode"

	"cloud.google.com/go/storage"
)

func makeStringSqlFriendly(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))

	var b strings.Builder
	for _, r := range s {

		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}

	return b.String()
}

// helpers to test whether a string value can be represented by a given BigQuery primitive type (no date/time inference)
func isInteger(s string) bool {
	if s == "" {
		return true
	}
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func isFloat(s string) bool {
	if s == "" {
		return true
	}
	// exclude values that are integers when checking float compatibility? Not necessary; integers are valid floats too
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func isBoolean(s string) bool {
	if s == "" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "t", "1", "false", "f", "0", "yes", "no", "y", "n":
		return true
	default:
		return false
	}
}

// Intentionally no date/time helpers

type bqField struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Mode string `json:"mode"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

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

	columnNames, err := inFileReader.Read()
	if err != nil {
		logger.Error("Failed to read infile", "error", err)
		os.Exit(1)
	}

	// Normalize column names to be SQL-friendly (for output header)
	normalizedColumnNames := make([]string, len(columnNames))
	for i := range columnNames {
		normalizedColumnNames[i] = makeStringSqlFriendly(columnNames[i])
	}

	logger.Info("Column names", "columnNames", normalizedColumnNames)

	outFileWriter.Write(normalizedColumnNames)
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

	colCandidates := make([]candidate, len(normalizedColumnNames))
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
	finalTypes := make([]string, len(normalizedColumnNames))
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
	schema := make([]bqField, 0, len(normalizedColumnNames))
	for i, name := range normalizedColumnNames {
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

}
