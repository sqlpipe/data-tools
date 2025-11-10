package main

import (
	"strconv"
	"strings"
	"unicode"
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
