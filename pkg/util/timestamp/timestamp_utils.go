package timestamp

import (
	"fmt"
	"time"
)

// Common PostgreSQL timestamp formats
var pgTimestampFormats = []string{
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
	for _, format := range pgTimestampFormats {
		if _, err := time.Parse(format, s); err == nil {
			return true
		}
	}
	return false
}

// ConvertPgTimestamp attempts to parse a PostgreSQL timestamp string into a time.Time
func ConvertPgTimestamp(s string) (time.Time, error) {
	for _, format := range pgTimestampFormats {
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
