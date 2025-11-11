// cloudRunCsvCleaner: cleans CSVs, optionally normalizes headers, infers or validates
// primitive types, and writes the cleaned CSV plus a BigQuery schema JSON to GCS.
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

	// MOUNT_PATH: Base directory path where input files are mounted (e.g., Cloud Run volume mount)
	mountPath := os.Getenv("MOUNT_PATH")
	if mountPath == "" {
		mountPath = "/gcs"
		logger.Info("Mount path is not set, using default /gcs")
	}

	// IN_FILE: Filename (not full path) of the input CSV file relative to MOUNT_PATH
	inFileName := os.Getenv("IN_FILE")
	if inFileName == "" {
		logger.Error("IN_FILE env var is required")
		os.Exit(1)
	}

	// OUT_FILE: GCS object path (bucket-relative) where the cleaned CSV will be written
	outObjectPath := os.Getenv("OUT_FILE")
	if outObjectPath == "" {
		logger.Error("OUT_FILE env var is required")
		os.Exit(1)
	}

	// SCHEMA_FILE: GCS object path where the BigQuery schema JSON will be written
	schemaObjectPath := os.Getenv("SCHEMA_FILE")
	if schemaObjectPath == "" {
		logger.Error("SCHEMA_FILE env var is required")
		os.Exit(1)
	}

	// BUCKET: GCS bucket name for all output operations
	bucketName := os.Getenv("BUCKET")
	if bucketName == "" {
		logger.Error("BUCKET env var is required")
		os.Exit(1)
	}

	// EXPORT_NIL_TRANSFORM: When enabled, exports a "nil-transform.json" file containing
	// the inferred schema transform that would reproduce the current processing.
	// This is useful for generating SCHEMA_TRANSFORM configs for future runs.
	exportNilTransform := strings.EqualFold(os.Getenv("EXPORT_NIL_TRANSFORM"), "true")
	if exportNilTransform {
		logger.Info("EXPORT_NIL_TRANSFORM enabled; exporting nil transform")
	}

	// DELETE_IN_FILE: When enabled, deletes the input file after successful processing.
	// Useful for cleanup in Cloud Run environments where files are mounted temporarily.
	deleteInFile := strings.EqualFold(os.Getenv("DELETE_IN_FILE"), "true")
	if deleteInFile {
		logger.Info("DELETE_IN_FILE enabled; deleting infile")
	}

	// MAKE_COLUMN_NAMES_SQL_FRIENDLY: When enabled, transforms column names to be SQL-safe
	// by lowercasing and replacing non-alphanumeric characters with underscores.
	// Only applies when SCHEMA_TRANSFORM is not used (transform mode handles renaming separately).
	makeColumnNamesSqlFriendly := strings.EqualFold(os.Getenv("MAKE_COLUMN_NAMES_SQL_FRIENDLY"), "true")
	if makeColumnNamesSqlFriendly {
		logger.Info("MAKE_COLUMN_NAMES_SQL_FRIENDLY enabled; making column names SQL friendly")
	}

	// EXPORT_HEAD: When true, export a small CSV with just cleaned headers and first 5 rows
	// This provides a quick preview of the cleaned data without downloading the full file.
	exportHead := strings.EqualFold(os.Getenv("EXPORT_HEAD"), "true")
	if exportHead {
		logger.Info("EXPORT_HEAD enabled; exporting head")
	}

	// REMOVE_VALUES: Comma-separated list of exact string values to remove from cells.
	// Matching cells are replaced with empty strings. Useful for removing placeholder values
	// like "NULL", "N/A", etc. that should be treated as empty rather than literal strings.
	removeValuesEnv := os.Getenv("REMOVE_VALUES")
	if removeValuesEnv != "" {
		logger.Info("REMOVE_VALUES env var is set; removing values", "remove-values", removeValuesEnv)
	}
	removeValues := strings.Split(removeValuesEnv, ",")

	// Build a map for O(1) lookup when checking if values should be removed.
	// Empty tokens are skipped to avoid treating all empty cells as matches.
	removeValuesMap := make(map[string]bool)
	for _, value := range removeValues {
		if value == "" {
			continue // ignore empty tokens to avoid treating all empty cells as matches
		}
		removeValuesMap[value] = true
	}

	// Construct full path to input file by combining mount path and filename
	infilePath := fmt.Sprintf("%s/%s", mountPath, inFileName)

	// Open the input CSV file for reading
	inFile, err := os.Open(infilePath)
	if err != nil {
		logger.Error("Failed to open infile", "error", err)
		os.Exit(1)
	}
	defer inFile.Close()

	// We'll read the first line as a raw string when SCHEMA_TRANSFORM is set
	// to allow substring replacements that may include commas.
	// After header handling, we will construct a csv.Reader for the remaining rows.
	// Using bufio.Reader allows us to read the header as raw text, then reuse the buffer
	// for CSV parsing of data rows, avoiding the need to re-open the file.
	bufReader := bufio.NewReader(inFile)
	rawHeaderLine, err := bufReader.ReadString('\n')
	if err != nil && err != io.EOF { // allow EOF for single-line files
		logger.Error("Failed to read infile header line", "error", err)
		os.Exit(1)
	}
	// Trim trailing newlines (support \r\n and \n)
	// This handles both Unix (\n) and Windows (\r\n) line endings
	rawHeaderLine = strings.TrimRight(rawHeaderLine, "\r\n")

	// Optional schema transform
	// SCHEMA_TRANSFORM: JSON array of column transformation rules.
	// When provided, enables strict schema validation mode where:
	// - Column names must match expected names (case-insensitive)
	// - Column order must match transform order
	// - Column count must match transform count
	// - Cell values are validated against expected types
	// - Headers can be renamed via "rename-to" field
	schemaTransformEnv := os.Getenv("SCHEMA_TRANSFORM")
	useTransform := strings.TrimSpace(schemaTransformEnv) != ""
	var originalColumnNames []string // Original headers before any transformation
	var columnNames []string         // Final column names to use in output
	var expectedTypes []string       // BigQuery types to validate against (when useTransform is true)

	if useTransform {
		// Parse the SCHEMA_TRANSFORM JSON into a slice of schemaTransform structs
		var transforms []schemaTransform
		if err := json.Unmarshal([]byte(schemaTransformEnv), &transforms); err != nil {
			logger.Error("Failed to parse SCHEMA_TRANSFORM JSON", "error", err)
			os.Exit(1)
		}

		// Perform substring replacements on the raw header line using the transforms.
		// Replacement is exact, case-sensitive, and uses rename-to if provided, else expected name.
		// This approach allows renaming columns that may contain commas or other CSV special characters
		// by doing string replacement before CSV parsing. The replacement happens on the raw header string,
		// so it works even if column names contain commas (which would be quoted in proper CSV).
		replacedHeader := rawHeaderLine
		for _, tr := range transforms {
			expected := strings.TrimSpace(tr.ExpectedColumnName)
			if expected == "" {
				logger.Error("SCHEMA_TRANSFORM entry missing expected-column-name")
				os.Exit(1)
			}
			// Use RenameTo if provided, otherwise keep the expected name
			target := strings.TrimSpace(tr.RenameTo)
			if target == "" {
				target = expected
			}
			replacedHeader = strings.ReplaceAll(replacedHeader, expected, target)
		}

		// Parse the replaced header using CSV rules to get final header tokens
		// This handles proper CSV parsing including quoted fields, escaped quotes, etc.
		headerReader := csv.NewReader(strings.NewReader(replacedHeader))
		parsedHeader, err := headerReader.Read()
		if err != nil {
			logger.Error("Failed to parse transformed header as CSV", "error", err)
			os.Exit(1)
		}
		originalColumnNames = parsedHeader

		// Validate exact match in number and names (in order) against transforms
		// The transform array defines the expected schema, so column count must match exactly.
		if len(parsedHeader) != len(transforms) {
			logger.Error("Header column count does not match SCHEMA_TRANSFORM", "headerCount", len(parsedHeader), "transformCount", len(transforms))
			os.Exit(1)
		}

		// Build final column names and expected types arrays, validating each column matches its transform
		columnNames = make([]string, len(parsedHeader))
		expectedTypes = make([]string, len(parsedHeader))
		for i, hdr := range parsedHeader {
			// Determine the desired name (RenameTo takes precedence over ExpectedColumnName)
			desiredName := strings.TrimSpace(transforms[i].RenameTo)
			if desiredName == "" {
				desiredName = strings.TrimSpace(transforms[i].ExpectedColumnName)
			}
			// Validate that the parsed header matches the expected/renamed name (case-insensitive)
			// This ensures the CSV structure matches the transform specification
			if !strings.EqualFold(hdr, desiredName) {
				logger.Error("Header column does not match SCHEMA_TRANSFORM at position", "index", i, "header", hdr, "expected", desiredName)
				os.Exit(1)
			}
			columnNames[i] = desiredName
			// Normalize the type string to a canonical BigQuery type (e.g., "INT" -> "INTEGER")
			expectedTypes[i] = normalizeBqType(transforms[i].Type)
		}
		logger.Info("Using SCHEMA_TRANSFORM for columns", "column-names", columnNames)
	} else {
		// No transform: parse header normally then optionally make SQL friendly names
		// In this mode, we accept whatever columns are in the CSV and optionally normalize their names.
		headerReader := csv.NewReader(strings.NewReader(rawHeaderLine))
		parsedHeader, err := headerReader.Read()
		if err != nil {
			logger.Error("Failed to parse header as CSV", "error", err)
			os.Exit(1)
		}
		originalColumnNames = parsedHeader
		// Start with original column names
		columnNames = make([]string, len(parsedHeader))
		copy(columnNames, parsedHeader)
		// Optionally transform column names to be SQL-safe (lowercase, alphanumeric + underscores)
		if makeColumnNamesSqlFriendly {
			for i := range columnNames {
				columnNames[i] = makeStringSqlFriendly(columnNames[i])
			}
		}
		logger.Info("Column names", "columnNames", columnNames)
	}

	// Initialize context, GCS client and writers AFTER header/transform is finalized
	// We delay GCS initialization until after header processing to avoid creating writers
	// if header validation fails early. This also ensures we have the final column names
	// before writing any output.
	ctx := context.Background()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		logger.Error("Failed to create storage client", "error", err)
		os.Exit(1)
	}
	defer storageClient.Close()

	// Create GCS writer for the main cleaned CSV output
	// The writer buffers data and uploads to GCS when closed or flushed
	gcsWriter := storageClient.Bucket(bucketName).Object(outObjectPath).NewWriter(ctx)
	gcsWriter.ContentType = "text/csv"
	defer func() {
		if err := gcsWriter.Close(); err != nil {
			logger.Error("Failed to close GCS writer", "error", err)
			os.Exit(1)
		}
	}()

	// Wrap the GCS writer with a CSV writer for proper CSV formatting
	outFileWriter := csv.NewWriter(gcsWriter)

	// Optional head export writer (header + first 5 rows)
	// Creates a separate CSV file containing only the header and first 5 data rows for quick preview
	var headCsvWriter *csv.Writer
	var headGcsWriter *storage.Writer
	if exportHead {
		// Append ".head.csv" to the output path to create the preview file
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

	// Write the header row to the main output CSV
	outFileWriter.Write(columnNames)
	// Also write header to head CSV if enabled
	if exportHead {
		headCsvWriter.Write(columnNames)
		headCsvWriter.Flush()
		if err := headCsvWriter.Error(); err != nil {
			logger.Error("Failed to write header to head CSV", "error", err)
			os.Exit(1)
		}
	}
	// Flush the main writer to ensure header is written immediately
	outFileWriter.Flush()

	// Check for any write errors after flushing
	err = outFileWriter.Error()
	if err != nil {
		logger.Error("Failed to flush outfile writer", "error", err)
		os.Exit(1)
	}

	// Pre-compute whether value removal is needed to avoid map lookup overhead when disabled
	removeValuesBool := len(removeValuesMap) > 0

	// Track possible types only when no schema transform provided
	// We evaluate against primitive candidates (no date/time)
	// Type inference uses a "candidate elimination" approach: start with all types as possible,
	// then eliminate candidates as we encounter values that don't match. This allows us to
	// infer the most specific type that fits all values in a column.
	type candidate struct {
		isInt   bool // Can this column be INTEGER?
		isFloat bool // Can this column be FLOAT?
		isBool  bool // Can this column be BOOLEAN?
	}

	colCandidates := make([]candidate, len(columnNames))
	if !useTransform {
		// Initialize all columns as candidates for all primitive types
		// As we process rows, we'll eliminate types that don't match observed values
		for i := range colCandidates {
			colCandidates[i] = candidate{
				isInt:   true,
				isFloat: true,
				isBool:  true,
			}
		}
	}

	lineNumber := 0 // Track current row number for error reporting and progress logging

	headRowsWritten := 0 // Counter for head CSV export (max 5 rows)
	// Create csv reader for the remaining rows starting after the header line we consumed
	// The bufReader still has the file position after the header, so we can read data rows directly
	inFileReader := csv.NewReader(bufReader)

	// Main processing loop: read each CSV row, clean/validate, and write to output
	for {

		// Log progress every 100,000 rows to track processing of large files
		if lineNumber%100000 == 0 {
			logger.Info("Reading CSV row", "line", lineNumber)
		}

		lineNumber++ // Increment before processing (line 1 is first data row after header)

		// Read the next CSV record (row)
		rec, err := inFileReader.Read()
		if err == io.EOF {
			break // End of file reached
		}
		if err != nil {
			logger.Error("Failed reading CSV record", "error", err)
			os.Exit(1)
		}

		// Remove values and update type candidates / enforce transform in a single pass
		// Process each cell in the row to:
		// 1. Remove specified values (replace with empty string)
		// 2. Validate types (if transform mode) or update type candidates (if inference mode)
		for i, v := range rec {
			// Check if this value should be removed (replaced with empty string)
			if removeValuesBool {
				if removeValuesMap[v] {
					rec[i] = "" // Replace matching value with empty string
					continue    // Skip type checking for removed values
				}
			}
			// Trim whitespace for type checking (but keep original in output if not removed)
			val := strings.TrimSpace(v)
			if val == "" {
				// empty values are always allowed (they become NULL in BigQuery)
				continue
			}
			if useTransform {
				// Transform mode: validate that the value conforms to the expected type
				// Fail fast if any value doesn't match the schema specification
				if !valueConformsToType(val, expectedTypes[i]) {
					logger.Error("Type validation failed", "line", lineNumber, "column", columnNames[i], "expectedType", expectedTypes[i], "value", val)
					os.Exit(1)
				}
			} else {
				// Inference mode: update type candidates by eliminating types that don't match
				// Once a type is eliminated (set to false), it stays false for that column
				c := colCandidates[i]
				if c.isInt && !isInteger(val) {
					c.isInt = false // This value isn't an integer, so column can't be INTEGER
				}
				if c.isFloat && !isFloat(val) {
					c.isFloat = false // This value isn't a float, so column can't be FLOAT
				}
				if c.isBool && !isBoolean(val) {
					c.isBool = false // This value isn't a boolean, so column can't be BOOLEAN
				}
				colCandidates[i] = c
			}
		}

		// Write the processed row to the main output CSV
		outFileWriter.Write(rec)
		// Also write to head CSV if enabled and we haven't reached 5 rows yet
		if exportHead && headRowsWritten < 5 {
			headCsvWriter.Write(rec)
			headRowsWritten++
		}
	}

	// Flush any remaining buffered CSV data to ensure all rows are written
	outFileWriter.Flush()

	err = outFileWriter.Error()
	if err != nil {
		logger.Error("Failed to flush outfile writer", "error", err)
		os.Exit(1)
	}

	// Flush head CSV writer if it was created
	if exportHead {
		headCsvWriter.Flush()
		if err := headCsvWriter.Error(); err != nil {
			logger.Error("Failed to flush head CSV writer", "error", err)
			os.Exit(1)
		}
	}

	// Determine final BigQuery types for schema generation
	// In transform mode, use the types from the transform specification.
	// In inference mode, select the most specific type that fits all values.
	finalTypes := make([]string, len(columnNames))
	if useTransform {
		// Use the expected types from the transform specification
		copy(finalTypes, expectedTypes)
	} else {
		// Infer types based on what candidates remain after processing all rows
		// Type preference order: INTEGER > FLOAT > BOOLEAN > STRING
		// This ensures we get the most specific type possible (e.g., INTEGER over FLOAT)
		for i, c := range colCandidates {
			// Prefer numeric, then bool, else string
			switch {
			case c.isInt:
				finalTypes[i] = "INTEGER" // Most specific numeric type
			case c.isFloat:
				finalTypes[i] = "FLOAT" // Numeric but not integer
			case c.isBool:
				finalTypes[i] = "BOOLEAN" // Boolean type
			default:
				finalTypes[i] = "STRING" // Fallback: any value that doesn't match primitives
			}
		}
	}

	// Build BigQuery schema JSON (array of fields)
	// The schema is a JSON array where each field has: name, type, and mode (NULLABLE/REQUIRED/REPEATED)
	// All fields are marked as NULLABLE since empty values are allowed in the CSV
	schema := make([]bqField, 0, len(columnNames))
	for i, name := range columnNames {
		fieldType := finalTypes[i]
		if fieldType == "" {
			fieldType = "STRING" // Default to STRING if type is somehow empty
		}
		// Create a BigQuery field with NULLABLE mode (allows NULL values)
		schema = append(schema, bqField{Name: name, Type: fieldType, Mode: "NULLABLE"})
	}

	// Marshal the schema to pretty-printed JSON (2-space indent)
	schemaBytes, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		logger.Error("Failed to marshal schema JSON", "error", err)
		os.Exit(1)
	}

	// Write schema JSON to GCS
	// This schema file can be used directly with BigQuery load jobs or bq command-line tool
	schemaWriter := storageClient.Bucket(bucketName).Object(schemaObjectPath).NewWriter(ctx)
	schemaWriter.ContentType = "application/json"
	if _, err := schemaWriter.Write(schemaBytes); err != nil {
		logger.Error("Failed to write schema to GCS", "error", err)
		_ = schemaWriter.Close() // Attempt cleanup even on error
		os.Exit(1)
	}
	if err := schemaWriter.Close(); err != nil {
		logger.Error("Failed to close schema writer", "error", err)
		os.Exit(1)
	}

	// Optionally export a nil transform JSON next to the schema JSON
	// This generates a SCHEMA_TRANSFORM configuration that would reproduce the current processing.
	// Useful for: capturing inferred schemas, documenting transformations, or creating reusable configs.
	if exportNilTransform {
		// Map BQ primitive types to transform types (lowercase)
		// The transform format uses lowercase type names: "number", "boolean", "string"
		// This function converts BigQuery types to the transform format
		mapBqToTransformType := func(bqType string) string {
			switch strings.ToUpper(strings.TrimSpace(bqType)) {
			case "INTEGER", "FLOAT", "NUMERIC", "DECIMAL", "NUMBER", "DOUBLE":
				return "number" // All numeric types map to "number"
			case "BOOLEAN", "BOOL":
				return "boolean" // Boolean types map to "boolean"
			default:
				return "string" // Everything else (including STRING) maps to "string"
			}
		}

		// Build transforms using original headers as expected names and current columnNames as rename targets
		// This creates a transform that would:
		// - Expect the original column names (as they appeared in the input CSV)
		// - Rename them to the current column names (after SQL-friendly transformation if applied)
		// - Use the inferred/validated types
		transforms := make([]schemaTransform, 0, len(columnNames))
		for i := range columnNames {
			transforms = append(transforms, schemaTransform{
				ExpectedColumnName: originalColumnNames[i],              // Original header from input CSV
				Type:               mapBqToTransformType(finalTypes[i]), // Inferred/validated type
				RenameTo:           columnNames[i],                      // Final column name (may differ if SQL-friendly transform was applied)
			})
		}

		// Marshal the transforms to pretty-printed JSON
		transformBytes, err := json.MarshalIndent(transforms, "", "  ")
		if err != nil {
			logger.Error("Failed to marshal nil transform JSON", "error", err)
			os.Exit(1)
		}

		// Derive destination object path: same directory as schemaObjectPath
		// Extract the directory path from schemaObjectPath and append "nil-transform.json"
		transformObjectPath := func() string {
			idx := strings.LastIndex(schemaObjectPath, "/")
			if idx == -1 {
				// No directory separator found, use root-level file
				return "nil-transform.json"
			}
			// Place in same directory as schema file
			return schemaObjectPath[:idx+1] + "nil-transform.json"
		}()

		// Write the transform JSON to GCS
		transformWriter := storageClient.Bucket(bucketName).Object(transformObjectPath).NewWriter(ctx)
		transformWriter.ContentType = "application/json"
		if _, err := transformWriter.Write(transformBytes); err != nil {
			logger.Error("Failed to write nil transform to GCS", "error", err)
			_ = transformWriter.Close() // Attempt cleanup even on error
			os.Exit(1)
		}
		if err := transformWriter.Close(); err != nil {
			logger.Error("Failed to close nil transform writer", "error", err)
			os.Exit(1)
		}

		logger.Info("Nil transform written", "bucket", bucketName, "object", transformObjectPath)
	}

	// Log successful completion with output locations
	logger.Info("CSV and schema written", "bucket", bucketName, "csvObject", outObjectPath, "schemaObject", schemaObjectPath)

	// Optionally delete the input file after successful processing
	// This is useful for cleanup in Cloud Run environments where files are mounted temporarily
	// and should be removed after processing to free up space
	if deleteInFile {
		err = os.Remove(infilePath)
		// Ignore "file not found" errors (file may have been deleted already)
		if err != nil && !os.IsNotExist(err) {
			logger.Error("Failed to delete infile", "error", err)
			os.Exit(1)
		}
		logger.Info("Deleted infile", "file", infilePath)
	}
}
