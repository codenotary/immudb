package engine

import (
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/server"
)

func TestParseTimestamp(t *testing.T) {
	now := time.Now().UTC()
	formatted := now.Format("2006-01-02 15:04:05.999999")

	// Test standard PostgreSQL timestamp format
	t.Run("PostgreSQL format", func(t *testing.T) {
		pgTimestamp := "2022-03-18 10:23:15"

		// Test detection
		if !server.IsPgTimestampString(pgTimestamp) {
			t.Errorf("Expected %s to be detected as a timestamp", pgTimestamp)
		}

		// Test conversion
		ts, err := server.ConvertPgTimestamp(pgTimestamp)
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
		if !server.IsPgTimestampString(isoTimestamp) {
			t.Errorf("Expected %s to be detected as a timestamp", isoTimestamp)
		}

		// Test conversion
		ts, err := server.ConvertPgTimestamp(isoTimestamp)
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
		if !server.IsPgTimestampString(pgTimestamp) {
			t.Errorf("Expected %s to be detected as a timestamp", pgTimestamp)
		}

		// Test conversion
		ts, err := server.ConvertPgTimestamp(pgTimestamp)
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
		if server.IsPgTimestampString(invalidTimestamp) {
			t.Errorf("Expected %s to NOT be detected as a timestamp", invalidTimestamp)
		}

		// Test conversion
		_, err := server.ConvertPgTimestamp(invalidTimestamp)
		if err == nil {
			t.Errorf("Expected error when parsing %s", invalidTimestamp)
		}
	})
}
