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
package document

import (
	"errors"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
)

func TestMayTranslateError(t *testing.T) {
	errCustom := errors.New("custom error")
	// Test cases with different error inputs
	testCases := []struct {
		inputError error
		expected   error
	}{
		{nil, nil},
		{sql.ErrTableAlreadyExists, ErrCollectionAlreadyExists},
		{sql.ErrTableDoesNotExist, ErrCollectionDoesNotExist},
		{sql.ErrNoMoreRows, ErrNoMoreDocuments},
		{sql.ErrColumnAlreadyExists, ErrFieldAlreadyExists},
		{sql.ErrColumnDoesNotExist, ErrFieldDoesNotExist},
		{sql.ErrLimitedIndexCreation, ErrLimitedIndexCreation},
		{store.ErrTxReadConflict, ErrConflict},
		{store.ErrKeyAlreadyExists, ErrConflict},
		{errCustom, errCustom},
	}

	// Run the test cases
	for _, tc := range testCases {
		result := mayTranslateError(tc.inputError)
		if result != tc.expected {
			t.Errorf("Error translation mismatch. Input: %v, Expected: %v, Got: %v", tc.inputError, tc.expected, result)
		}
	}
}
