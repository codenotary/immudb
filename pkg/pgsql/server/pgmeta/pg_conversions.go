package pgmeta

import (
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"strings"
	"time"
)

// ConvertPgTimestampParam converts a timestamp parameter from the PostgreSQL JDBC driver
// format to a format that immudb can understand and process
func ConvertPgTimestampParam(param string) (*schema.SQLValue, error) {
	if param == "" {
		return nil, fmt.Errorf("empty timestamp parameter")
	}

	// Remove any PostgreSQL type annotations that might be present
	if strings.HasPrefix(param, "::timestamp") {
		param = strings.TrimPrefix(param, "::timestamp")
	}

	// Handle special PostgreSQL timestamp formats
	param = strings.TrimSpace(param)

	// Check for ISO format with fractional seconds and timezone
	// The PostgreSQL JDBC driver often sends timestamps in this format
	if strings.Contains(param, "T") && (strings.Contains(param, "Z") || strings.Contains(param, "+") || strings.Contains(param, "-")) {
		if t, err := time.Parse(time.RFC3339Nano, param); err == nil {
			return &schema.SQLValue{
				Value: &schema.SQLValue_Ts{
					Ts: &schema.Timestamp{
						Seconds: t.Unix(),
						Nanos:   int32(t.Nanosecond()),
					},
				},
			}, nil
		}
	}

	timestamp, err := server.ConvertPgTimestamp(param)
	if err != nil {
		return nil, err
	}

	return &schema.SQLValue{
		Value: &schema.SQLValue_Ts{
			Ts: &schema.Timestamp{
				Seconds: timestamp.Unix(),
				Nanos:   int32(timestamp.Nanosecond()),
			},
		},
	}, nil
}

// FormatTimestampForPg formats a timestamp for returning to PostgreSQL clients
func FormatTimestampForPg(ts *schema.Timestamp) string {
	t := time.Unix(ts.Seconds, int64(ts.Nanos))
	return t.Format("2006-01-02 15:04:05.999999")
}

// HandlePgTimestampLiterals processes SQL queries to handle PostgreSQL timestamp literals
// like '2022-01-01 12:00:00'::timestamp
func HandlePgTimestampLiterals(query string) string {
	// Remove PostgreSQL timestamp type casts
	query = strings.ReplaceAll(query, "::timestamp", "")

	// Handle CURRENT_TIMESTAMP and NOW() functions
	now := time.Now().Format("2006-01-02 15:04:05.999999")
	query = strings.ReplaceAll(query, "CURRENT_TIMESTAMP", fmt.Sprintf("'%s'", now))
	query = strings.ReplaceAll(query, "NOW()", fmt.Sprintf("'%s'", now))

	return query
}
