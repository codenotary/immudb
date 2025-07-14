package server

import (
	"strings"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
)

// ProcessSQLQueryForPgCompat processes an SQL query for PostgreSQL compatibility
// including handling timestamp literals in the query
func ProcessSQLQueryForPgCompat(query string, params []*schema.NamedParam) (string, []*schema.NamedParam, error) {
	// Process query for timestamp literal patterns
	modifiedQuery := query

	// Find timestamp literals like '2022-01-01 12:00:00'::timestamp
	// and convert them to a format immudb understands

	// Process parameters that might contain timestamp values
	if params != nil {
		modifiedParams := make([]*schema.NamedParam, len(params))
		for i, param := range params {
			if param.Value != nil {
				// Check if this might be a string that represents a timestamp
				if strVal, ok := param.Value.Value.(*schema.SQLValue_S); ok {
					if server.IsPgTimestampString(strVal.S) {
						tsValue, err := server.ConvertToImmudbTimestamp(strVal.S)
						if err == nil {
							modifiedParam := *param
							modifiedParam.Value = tsValue
							modifiedParams[i] = &modifiedParam
							continue
						}
					}
				}
			}
			modifiedParams[i] = param
		}
		params = modifiedParams
	}

	// Replace timestamp casting in query with immudb compatible syntax
	modifiedQuery = strings.ReplaceAll(modifiedQuery, "::timestamp", "")

	return modifiedQuery, params, nil
}
