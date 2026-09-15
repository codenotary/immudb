package server

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
	"github.com/codenotary/immudb/pkg/server"
	"strconv"
	"strings"
)

// HandlePreparedStatementParams processes parameters for prepared statements
// with special handling for timestamp parameters which need conversion
func HandlePreparedStatementParams(paramTypes []int32, paramValues []string) ([]*schema.SQLValue, error) {
	values := make([]*schema.SQLValue, len(paramValues))

	for i, paramValue := range paramValues {
		// Check if this is a timestamp parameter based on its type
		// PostgreSQL timestamp type OID is 1114 (TIMESTAMP) or 1184 (TIMESTAMPTZ)
		if paramTypes[i] == 1114 || paramTypes[i] == 1184 {
			// First try the existing conversion
			sqlValue, err := pgmeta.ConvertPgTimestampParam(paramValue)
			if err != nil {
				// If that fails, try additional format handling

				// If it looks like a numeric value, try parsing as epoch milliseconds
				if strings.Trim(paramValue, "0123456789") == "" {
					if ms, err := strconv.ParseInt(paramValue, 10, 64); err == nil {
						tsValue, err := server.ConvertToImmudbTimestamp(ms)
						if err == nil {
							values[i] = tsValue
							continue
						}
					}
				}

				// Try if it's a standard timestamp string
				if server.IsPgTimestampString(paramValue) {
					tsValue, err := server.ConvertToImmudbTimestamp(paramValue)
					if err == nil {
						values[i] = tsValue
						continue
					}
				}

				// If all else fails, return the original error
				return nil, err
			}
			values[i] = sqlValue
		} else {
			// For other types, use the regular parameter handling
			values[i] = &schema.SQLValue{
				Value: &schema.SQLValue_S{
					S: paramValue,
				},
			}
		}
	}

	return values, nil
}
