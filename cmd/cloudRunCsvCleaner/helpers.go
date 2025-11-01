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
