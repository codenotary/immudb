package server

import (
	"time"
	"github.com/codenotary/immudb/pkg/api/schema"
)

// ConvertToImmudbTimestamp takes a value that might be a PostgreSQL timestamp and
// converts it to a format that immudb can use in SQL queries
func ConvertToImmudbTimestamp(value interface{}) (*schema.SQLValue, error) {
	switch v := value.(type) {
	case time.Time:
		// Already a time.Time, no conversion needed
		return &schema.SQLValue{
			Value: &schema.SQLValue_Ts{
				Ts: &schema.Timestamp{
					Seconds: v.Unix(),
					Nanos:   int32(v.Nanosecond()),
				},
			},
		}, nil
	case string:
		// Check if this might be a PostgreSQL timestamp string
		if IsPgTimestampString(v) {
			timestamp, err := ConvertPgTimestamp(v)
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
		// Not a timestamp string, pass through
		return &schema.SQLValue{
			Value: &schema.SQLValue_S{
				S: v,
			},
		}, nil
	case int64:
		// Handle epoch milliseconds (used by some JDBC drivers)
		t := time.Unix(v/1000, (v%1000)*1000000)
		return &schema.SQLValue{
			Value: &schema.SQLValue_Ts{
				Ts: &schema.Timestamp{
					Seconds: t.Unix(),
					Nanos:   int32(t.Nanosecond()),
				},
			},
		}, nil
	default:
		// Let the original handler deal with it
		return nil, nil
	}
}
