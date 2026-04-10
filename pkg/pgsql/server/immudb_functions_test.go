/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsEmulableInternally_ImmudbState(t *testing.T) {
	s := &session{}

	tests := []struct {
		name      string
		statement string
		match     bool
	}{
		{"basic", "SELECT immudb_state()", true},
		{"lowercase", "select immudb_state()", true},
		{"spaces", "SELECT  immudb_state(  )", true},
		{"mixed case", "Select Immudb_State()", true},
		{"no match", "SELECT something_else()", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.isEmulableInternally(tt.statement)
			if tt.match {
				_, ok := result.(*immudbStateCmd)
				require.True(t, ok, "expected immudbStateCmd")
			} else {
				require.Nil(t, result)
			}
		})
	}
}

func TestIsEmulableInternally_ImmudbVerifyRow(t *testing.T) {
	s := &session{}

	tests := []struct {
		name      string
		statement string
		match     bool
		args      string
	}{
		{"basic", "SELECT immudb_verify_row('mytable', 1)", true, "'mytable', 1"},
		{"lowercase", "select immudb_verify_row('t', 42)", true, "'t', 42"},
		{"multi pk", "SELECT immudb_verify_row('orders', 1, 'abc')", true, "'orders', 1, 'abc'"},
		{"no match", "SELECT verify_row()", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.isEmulableInternally(tt.statement)
			if tt.match {
				cmd, ok := result.(*immudbVerifyRowCmd)
				require.True(t, ok, "expected immudbVerifyRowCmd")
				require.Equal(t, tt.args, cmd.args)
			} else {
				require.Nil(t, result)
			}
		})
	}
}

func TestIsEmulableInternally_ImmudbVerifyTx(t *testing.T) {
	s := &session{}

	tests := []struct {
		name      string
		statement string
		match     bool
		args      string
	}{
		{"basic", "SELECT immudb_verify_tx(1)", true, "1"},
		{"large id", "SELECT immudb_verify_tx(12345)", true, "12345"},
		{"no match", "SELECT verify_tx()", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.isEmulableInternally(tt.statement)
			if tt.match {
				cmd, ok := result.(*immudbVerifyTxCmd)
				require.True(t, ok, "expected immudbVerifyTxCmd")
				require.Equal(t, tt.args, cmd.args)
			} else {
				require.Nil(t, result)
			}
		})
	}
}

func TestIsEmulableInternally_ImmudbHistory(t *testing.T) {
	s := &session{}

	tests := []struct {
		name      string
		statement string
		match     bool
		args      string
	}{
		{"basic", "SELECT immudb_history('mykey')", true, "'mykey'"},
		{"no match", "SELECT history()", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.isEmulableInternally(tt.statement)
			if tt.match {
				cmd, ok := result.(*immudbHistoryCmd)
				require.True(t, ok, "expected immudbHistoryCmd")
				require.Equal(t, tt.args, cmd.args)
			} else {
				require.Nil(t, result)
			}
		})
	}
}

func TestIsEmulableInternally_ImmudbTx(t *testing.T) {
	s := &session{}

	tests := []struct {
		name      string
		statement string
		match     bool
		args      string
	}{
		{"basic", "SELECT immudb_tx(1)", true, "1"},
		{"no match", "SELECT tx()", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.isEmulableInternally(tt.statement)
			if tt.match {
				cmd, ok := result.(*immudbTxCmd)
				require.True(t, ok, "expected immudbTxCmd")
				require.Equal(t, tt.args, cmd.args)
			} else {
				require.Nil(t, result)
			}
		})
	}
}

func TestParseVerifyArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      string
		table     string
		pkCount   int
		expectErr bool
	}{
		{"basic", "'mytable', 1", "mytable", 1, false},
		{"multi pk", "'orders', 1, 'abc'", "orders", 2, false},
		{"quoted table", "\"mytable\", 42", "mytable", 1, false},
		{"no pk", "'mytable'", "", 0, true},
		{"empty", "", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, pkValues, err := parseVerifyArgs(tt.args)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.table, table)
				require.Equal(t, tt.pkCount, len(pkValues))
			}
		})
	}
}

func TestTrimQuotes(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"'hello'", "hello"},
		{"\"hello\"", "hello"},
		{"hello", "hello"},
		{"''", ""},
		{"'", "'"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(t, tt.expected, trimQuotes(tt.input))
		})
	}
}

func TestIsEmulableInternally_Show(t *testing.T) {
	s := &session{}

	tests := []struct {
		name      string
		statement string
		match     bool
		param     string
	}{
		{"server_version", "SHOW server_version", true, "server_version"},
		{"lowercase", "show timezone", true, "timezone"},
		{"with semicolon", "SHOW client_encoding;", true, "client_encoding"},
		{"search_path", "SHOW search_path", true, "search_path"},
		{"not show", "SELECT 1", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.isEmulableInternally(tt.statement)
			if tt.match {
				cmd, ok := result.(*showCmd)
				require.True(t, ok, "expected showCmd")
				require.Equal(t, tt.param, cmd.param)
			} else {
				if result != nil {
					_, isShow := result.(*showCmd)
					require.False(t, isShow)
				}
			}
		})
	}
}

func TestExistingEmulationNotBroken(t *testing.T) {
	s := &session{}

	// version still works
	result := s.isEmulableInternally("SELECT version()")
	_, ok := result.(*version)
	require.True(t, ok)

	// blacklist still works
	require.True(t, s.isInBlackList("SET client_encoding TO 'UTF8'"))
	require.True(t, s.isInBlackList(";"))
	require.False(t, s.isInBlackList("SELECT 1"))

	// non-matching returns nil
	require.Nil(t, s.isEmulableInternally("SELECT 1"))
}
