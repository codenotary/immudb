/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package client

import (
	"errors"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestDecodeRowErrors(t *testing.T) {

	type tMap map[uint32]sql.SQLValueType

	for _, d := range []struct {
		n        string
		data     []byte
		colTypes map[uint32]sql.SQLValueType
	}{
		{
			"No data",
			nil,
			nil,
		},
		{
			"Short buffer",
			[]byte{1},
			tMap{},
		},
		{
			"Short buffer on type",
			[]byte{0, 0, 0, 1, 0, 0, 1},
			tMap{},
		},
		{
			"Missing type",
			[]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1},
			tMap{},
		},
		{
			"Invalid value",
			[]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0},
			tMap{
				1: sql.VarcharType,
			},
		},
	} {
		t.Run(d.n, func(t *testing.T) {
			row, err := decodeRow(d.data, d.colTypes)
			require.True(t, errors.Is(err, sql.ErrCorruptedData))
			require.Nil(t, row)
		})
	}
}

func TestVerifyAgainst(t *testing.T) {

	// Missing column type
	err := verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values:  []*schema.SQLValue{{Value: nil}},
	}, map[uint32]*schema.SQLValue{}, map[string]uint32{})
	require.True(t, errors.Is(err, sql.ErrColumnDoesNotExist))

	// Nil value
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values:  []*schema.SQLValue{{Value: nil}},
	}, map[uint32]*schema.SQLValue{}, map[string]uint32{
		"c1": 0,
	})
	require.True(t, errors.Is(err, sql.ErrCorruptedData))

	// Missing decoded value
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values: []*schema.SQLValue{
			{Value: &schema.SQLValue_N{N: 1}},
		},
	}, map[uint32]*schema.SQLValue{}, map[string]uint32{
		"c1": 0,
	})
	require.True(t, errors.Is(err, sql.ErrCorruptedData))

	// Invalid decoded value
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values: []*schema.SQLValue{
			{Value: &schema.SQLValue_N{N: 1}},
		},
	}, map[uint32]*schema.SQLValue{
		0: {Value: nil},
	}, map[string]uint32{
		"c1": 0,
	})
	require.True(t, errors.Is(err, sql.ErrCorruptedData))

	// Not comparable types
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values: []*schema.SQLValue{
			{Value: &schema.SQLValue_N{N: 1}},
		},
	}, map[uint32]*schema.SQLValue{
		0: {Value: &schema.SQLValue_S{S: "1"}},
	}, map[string]uint32{
		"c1": 0,
	})
	require.True(t, errors.Is(err, sql.ErrNotComparableValues))

	// Different values
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values: []*schema.SQLValue{
			{Value: &schema.SQLValue_N{N: 1}},
		},
	}, map[uint32]*schema.SQLValue{
		0: {Value: &schema.SQLValue_N{N: 2}},
	}, map[string]uint32{
		"c1": 0,
	})
	require.True(t, errors.Is(err, sql.ErrCorruptedData))

	// Successful verify
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values: []*schema.SQLValue{
			{Value: &schema.SQLValue_N{N: 1}},
		},
	}, map[uint32]*schema.SQLValue{
		0: {Value: &schema.SQLValue_N{N: 1}},
	}, map[string]uint32{
		"c1": 0,
	})
	require.NoError(t, err)
}
