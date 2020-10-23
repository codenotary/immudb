/*
Copyright 2019-2020 vChain, Inc.

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

package ahtree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAHtree(t *testing.T) {
	tree := &AHtree{}

	var nodesUptoTests = []struct {
		n        int64
		expected int64
	}{
		{1, 1},
		{2, 3},
		{3, 5},
		{4, 8},
		{5, 10},
		{6, 13},
		{7, 16},
		{8, 20},
		{9, 22},
		{10, 25},
		{11, 28},
		{12, 32},
		{13, 35},
		{14, 39},
		{15, 43},
		{16, 48},
	}

	for _, tt := range nodesUptoTests {
		actual := tree.nodesUpto(tt.n)
		require.Equal(t, tt.expected, actual)
	}

	_, _, err := tree.Append([]byte{1})
	require.NoError(t, err)

	_, _, err = tree.Append([]byte{2})
	require.NoError(t, err)

	_, _, err = tree.Append([]byte{3})
	require.NoError(t, err)

	_, _, err = tree.Append([]byte{4})
	require.NoError(t, err)

	_, _, err = tree.Append([]byte{5})
	require.NoError(t, err)

	_, _, err = tree.Append([]byte{6})
	require.NoError(t, err)

	_, _, err = tree.Append([]byte{7})
	require.NoError(t, err)

	_, _, err = tree.Append([]byte{8})
	require.NoError(t, err)

	_, _, err = tree.Append([]byte{9})
	require.NoError(t, err)
}
