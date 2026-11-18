package pgtime

import (
	"testing"
	"time"
)

func TestPgTimestampHandling(t *testing.T) {
	// Test standard PostgreSQL timestamp format
	t.Run("PostgreSQL format", func(t *testing.T) {
		pgTimestamp := "2022-03-18 10:23:15"

		// Test detection
		if !IsPgTimestampString(pgTimestamp) {
			t.Errorf("Expected %s to be detected as a timestamp", pgTimestamp)
		}

		// Test conversion
		ts, err := ConvertPgTimestamp(pgTimestamp)
		if err != nil {
			t.Errorf("Failed to parse %s: %v", pgTimestamp, err)
		}

		expected := time.Date(2022, 3, 18, 10, 23, 15, 0, time.UTC)
		if !ts.Equal(expected) {
			t.Errorf("Expected %v, got %v", expected, ts)
		}
	})

	// Test ISO 8601 format
	t.Run("ISO 8601 format", func(t *testing.T) {
		isoTimestamp := "2022-03-18T10:23:15Z"

		// Test detection
		if !IsPgTimestampString(isoTimestamp) {
			t.Errorf("Expected %s to be detected as a timestamp", isoTimestamp)
		}

		// Test conversion
		ts, err := ConvertPgTimestamp(isoTimestamp)
		if err != nil {
			t.Errorf("Failed to parse %s: %v", isoTimestamp, err)
		}

		expected := time.Date(2022, 3, 18, 10, 23, 15, 0, time.UTC)
		if !ts.Equal(expected) {
			t.Errorf("Expected %v, got %v", expected, ts)
		}
	})

	// Test PostgreSQL format with fractional seconds
	t.Run("PostgreSQL format with fractional seconds", func(t *testing.T) {
		pgTimestamp := "2022-03-18 10:23:15.123456"

		// Test detection
		if !IsPgTimestampString(pgTimestamp) {
			t.Errorf("Expected %s to be detected as a timestamp", pgTimestamp)
		}

		// Test conversion
		ts, err := ConvertPgTimestamp(pgTimestamp)
		if err != nil {
			t.Errorf("Failed to parse %s: %v", pgTimestamp, err)
		}

		expected := time.Date(2022, 3, 18, 10, 23, 15, 123456000, time.UTC)
		if !ts.Equal(expected) {
			t.Errorf("Expected %v, got %v", expected, ts)
		}
	})

	// Test invalid timestamp
	t.Run("Invalid timestamp", func(t *testing.T) {
		invalidTimestamp := "not-a-timestamp"

		// Test detection
		if IsPgTimestampString(invalidTimestamp) {
			t.Errorf("Expected %s to NOT be detected as a timestamp", invalidTimestamp)
		}

		// Test conversion
		_, err := ConvertPgTimestamp(invalidTimestamp)
		if err == nil {
			t.Errorf("Expected error when parsing %s", invalidTimestamp)
		}
	})

	// Test format timestamp
	t.Run("Format timestamp", func(t *testing.T) {
		ts := time.Date(2022, 3, 18, 10, 23, 15, 123456000, time.UTC)
		formatted := FormatTimestamp(ts)
		expected := "2022-03-18 10:23:15.123456"
		if formatted != expected {
			t.Errorf("Expected formatted timestamp %s, got %s", expected, formatted)
		}
	})

	// Test timestamp literal handling
	t.Run("Handle timestamp literals", func(t *testing.T) {
		query := "SELECT * FROM table WHERE timestamp_col = '2022-03-18 10:23:15'::timestamp"
		modified := HandlePgTimestampLiterals(query)
		expected := "SELECT * FROM table WHERE timestamp_col = '2022-03-18 10:23:15'"
		if modified != expected {
			t.Errorf("Expected modified query %s, got %s", expected, modified)
		}
	})
}
