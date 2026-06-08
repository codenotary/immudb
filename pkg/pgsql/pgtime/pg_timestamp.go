package pgtime

import (
	"fmt"
	"strings"
	"time"
)

// PostgreSQL timestamp formats that might be received from clients
var PgTimestampFormats = []string{
	"2006-01-02 15:04:05",           // YYYY-MM-DD HH:MM:SS
	"2006-01-02 15:04:05.999999",    // YYYY-MM-DD HH:MM:SS.SSSSSS
	"2006-01-02T15:04:05Z",          // ISO 8601
	"2006-01-02T15:04:05.999999Z",   // ISO 8601 with microseconds
	"2006-01-02T15:04:05-07:00",     // ISO 8601 with timezone
	"2006-01-02T15:04:05.999-07:00", // ISO 8601 with microseconds and timezone
	time.RFC3339,                    // RFC3339
	time.RFC3339Nano,                // RFC3339 with nanoseconds
}

// IsPgTimestampString checks if a string likely represents a PostgreSQL timestamp
func IsPgTimestampString(s string) bool {
	for _, format := range PgTimestampFormats {
		if _, err := time.Parse(format, s); err == nil {
			return true
		}
	}
	return false
}

// ConvertPgTimestamp attempts to parse a PostgreSQL timestamp string into a time.Time
func ConvertPgTimestamp(s string) (time.Time, error) {
	// First handle any PostgreSQL-specific formatting
	s = strings.TrimSpace(s)

	// Remove type annotations
	if strings.HasPrefix(s, "::timestamp") {
		s = strings.TrimPrefix(s, "::timestamp")
		s = strings.TrimSpace(s)
	}

	// Try all supported formats
	for _, format := range PgTimestampFormats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("value is not a timestamp: invalid value provided")
}

// FormatTimestamp formats a time.Time as a PostgreSQL-compatible timestamp string
func FormatTimestamp(t time.Time) string {
	return t.Format("2006-01-02 15:04:05.999999")
}

// HandlePgTimestampLiterals processes SQL queries to handle PostgreSQL timestamp literals
func HandlePgTimestampLiterals(query string) string {
	// Remove PostgreSQL timestamp type casts
	query = strings.ReplaceAll(query, "::timestamp", "")

	// Handle CURRENT_TIMESTAMP and NOW() functions
	now := FormatTimestamp(time.Now())
	query = strings.ReplaceAll(query, "CURRENT_TIMESTAMP", fmt.Sprintf("'%s'", now))
	query = strings.ReplaceAll(query, "NOW()", fmt.Sprintf("'%s'", now))

	return query
}
