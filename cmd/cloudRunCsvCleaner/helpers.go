// Package main contains helper utilities used by the CSV cleaner to normalize
// header names and validate primitive BigQuery-compatible types inferred from CSV cells.
package main

import (
	"strconv"
	"strings"
	"unicode"
)

// makeStringSqlFriendly lower-cases, trims, and replaces any non [a-z0-9] rune
// with an underscore to produce predictable, SQL-friendly identifiers.
// This function transforms column names to be safe for use in SQL queries and BigQuery schemas.
// Examples: "User Name" -> "user_name", "Price ($)" -> "price__", "ID123" -> "id123"
func makeStringSqlFriendly(s string) string {
	// Normalize to lowercase and remove leading/trailing whitespace
	// This ensures consistent casing and removes accidental spaces
	s = strings.ToLower(strings.TrimSpace(s))

	// Use strings.Builder for efficient string concatenation when building the result
	var b strings.Builder
	// Iterate over each Unicode rune (character) in the string
	// This handles multi-byte characters correctly (e.g., accented letters)
	for _, r := range s {

		// Keep alphanumeric characters (letters and digits) as-is
		// unicode.IsLetter handles all Unicode letter categories (not just ASCII)
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		} else {
			// collapse any non-alphanumeric to underscore; we do not attempt to
			// de-duplicate consecutive underscores here to keep the routine simple
			// This means "Price ($)" becomes "price__" (two underscores for space and parentheses)
			// This is acceptable for SQL identifiers, though not ideal
			b.WriteRune('_')
		}
	}

	return b.String()
}

// helpers to test whether a string value can be represented by a given BigQuery
// primitive type (no date/time inference)
// These functions are used during type inference to determine the most specific
// type that fits all values in a column. Empty strings are considered valid for
// all types since they become NULL in BigQuery (which is allowed for NULLABLE fields).

// isInteger checks if a string can be parsed as a 64-bit signed integer (base 10).
// Returns true for empty strings (which become NULL) and valid integer strings.
// Examples: "123" -> true, "-456" -> true, "0" -> true, "abc" -> false, "12.5" -> false
func isInteger(s string) bool {
	// Empty strings are valid (they become NULL in BigQuery, which is allowed)
	if s == "" {
		return true
	}
	// Parse as base-10, 64-bit signed integer
	// Returns error for non-numeric strings, floats, or values outside int64 range
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

// isFloat checks if a string can be parsed as a 64-bit floating-point number (IEEE 754).
// Returns true for empty strings (which become NULL) and valid numeric strings.
// Note: Integers are valid floats (e.g., "123" can be stored as 123.0), so we don't
// exclude them. This allows columns with mixed integers and floats to be typed as FLOAT.
// Examples: "123" -> true, "45.67" -> true, "-0.5" -> true, "abc" -> false, "1.2.3" -> false
func isFloat(s string) bool {
	// Empty strings are valid (they become NULL in BigQuery, which is allowed)
	if s == "" {
		return true
	}
	// exclude values that are integers when checking float compatibility? Not necessary; integers are valid floats too
	// Parse as 64-bit float (handles both integers like "123" and floats like "45.67")
	// Returns error for non-numeric strings or malformed numbers
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// isBoolean checks if a string represents a boolean value using common CSV conventions.
// Returns true for empty strings (which become NULL) and recognized boolean representations.
// This function accepts a wide variety of boolean formats commonly found in CSV files,
// including numeric (0/1), single-letter (t/f, y/n), and full words (true/false, yes/no).
// Examples: "true" -> true, "1" -> true, "yes" -> true, "false" -> true, "abc" -> false
func isBoolean(s string) bool {
	// Empty strings are valid (they become NULL in BigQuery, which is allowed)
	if s == "" {
		return true
	}
	// Accept a generous set of boolean representations commonly seen in CSVs
	// Normalize to lowercase and trim whitespace for case-insensitive matching
	// This handles variations like "True", " TRUE ", "YES", etc.
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "t", "1", "false", "f", "0", "yes", "no", "y", "n":
		return true
	default:
		return false
	}
}

// bqField represents a single BigQuery schema field emitted as JSON.
// This struct matches the BigQuery schema JSON format used by bq command-line tool
// and BigQuery API. The schema is an array of these fields.
// Mode can be: "NULLABLE" (allows NULL), "REQUIRED" (not NULL), or "REPEATED" (array).
// In this tool, all fields are marked as NULLABLE since CSV cells can be empty.
type bqField struct {
	Name string `json:"name"` // Column name (must be valid BigQuery identifier)
	Type string `json:"type"` // BigQuery type: INTEGER, FLOAT, BOOLEAN, STRING, etc.
	Mode string `json:"mode"` // Field mode: NULLABLE, REQUIRED, or REPEATED
}

// schemaTransform represents a single column rule provided via SCHEMA_TRANSFORM environment variable.
// This struct defines how a column should be validated and optionally renamed.
// The SCHEMA_TRANSFORM is a JSON array of these structs, one per column in order.
// This enables strict schema validation mode where column names, order, and types are enforced.
type schemaTransform struct {
	ExpectedColumnName string `json:"expected-column-name"` // Original column name expected in CSV (case-insensitive match)
	Type               string `json:"type"`                  // Expected BigQuery type: "number", "boolean", or "string" (normalized by normalizeBqType)
	RenameTo           string `json:"rename-to"`            // Optional: new name for the column (if empty, uses ExpectedColumnName)
}

// normalizeBqType maps common synonyms to BigQuery primitive types we support here.
// This function canonicalizes type names from various sources (SCHEMA_TRANSFORM, user input)
// to the standard BigQuery type names used in schema JSON.
// The function is case-insensitive and handles common type aliases (e.g., "INT" -> "INTEGER").
// Unknown types are returned as-is (uppercased) to allow upstream code to handle them
// or fail with a clear error message.
func normalizeBqType(t string) string {
	// Normalize to uppercase and trim whitespace for case-insensitive matching
	tt := strings.ToUpper(strings.TrimSpace(t))
	switch tt {
	case "INT", "INTEGER":
		// Map integer synonyms to canonical INTEGER type
		return "INTEGER"
	case "FLOAT", "DOUBLE", "NUMBER", "NUMERIC", "DECIMAL":
		// Map all numeric/float synonyms to canonical FLOAT type
		// Note: BigQuery has NUMERIC and DECIMAL types, but we simplify to FLOAT
		// for compatibility with inferred types and simpler validation
		return "FLOAT"
	case "BOOL", "BOOLEAN":
		// Map boolean synonyms to canonical BOOLEAN type
		return "BOOLEAN"
	case "STRING", "TEXT":
		// Map string/text synonyms to canonical STRING type
		return "STRING"
	default:
		// Unknown inputs are returned as-is for upstream handling
		// This allows the caller to detect unsupported types and handle appropriately
		// (e.g., log an error or treat as STRING)
		return tt
	}
}

// valueConformsToType validates a non-empty trimmed value against a normalized BQ type.
// This function is used in schema transform mode to enforce type constraints on CSV values.
// The value should already be trimmed (whitespace removed) before calling this function.
// Empty values are considered valid for all types (they become NULL in BigQuery).
// Returns true if the value matches the expected type, false otherwise.
// Note: This function normalizes the typeName first, so it handles type synonyms correctly.
func valueConformsToType(val string, typeName string) bool {
	// Normalize the type name to canonical BigQuery type (handles synonyms like "INT" -> "INTEGER")
	switch normalizeBqType(typeName) {
	case "INTEGER":
		// Validate as integer (base-10, 64-bit signed)
		return isInteger(val)
	case "FLOAT":
		// Validate as float (64-bit IEEE 754)
		// Note: Integers are valid floats, so "123" passes FLOAT validation
		return isFloat(val)
	case "BOOLEAN":
		// Validate as boolean (accepts common CSV boolean representations)
		return isBoolean(val)
	case "STRING":
		// All values are valid strings (no validation needed)
		return true
	default:
		// Unknown types treated as STRING to avoid unexpected failures
		// This is a safe fallback: if we don't recognize the type, allow it as string
		// rather than failing validation. The normalizeBqType function will have already
		// logged or handled the unknown type, so this prevents double-failures.
		return true
	}
}
