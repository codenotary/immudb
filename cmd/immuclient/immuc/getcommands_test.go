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

package immuc_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTxByID(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.VerifiedSet([]string{"key", "val"})
	require.NoError(t, err)

	msg, err := ic.Imc.GetTxByID([]string{"1"})
	require.NoError(t, err, "GetByIndex fail")
	require.Contains(t, msg, "hash", "GetByIndex failed")
}
func TestGet(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.Set([]string{"key", "val"})
	require.NoError(t, err)

	msg, err := ic.Imc.Get([]string{"key"})
	require.NoError(t, err, "GetKey fail")
	require.Contains(t, msg, "value", "GetKey failed")
}

func TestVerifiedGet(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.Set([]string{"key", "val"})
	require.NoError(t, err)

	msg, err := ic.Imc.VerifiedGet([]string{"key"})
	require.NoError(t, err, "VerifiedGet fail")
	require.Contains(t, msg, "value", "VerifiedGet failed")
}

func TestGetByRevision(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.Set([]string{"key", "value1"})
	require.NoError(t, err)

	_, err = ic.Imc.Set([]string{"key", "value2"})
	require.NoError(t, err)

	_, err = ic.Imc.Set([]string{"key", "value3"})
	require.NoError(t, err)

	msg, err := ic.Imc.Get([]string{"key@1"})
	require.NoError(t, err)
	require.Contains(t, msg, "value1")

	msg, err = ic.Imc.Get([]string{"key@2"})
	require.NoError(t, err)
	require.Contains(t, msg, "value2")

	msg, err = ic.Imc.Get([]string{"key@3"})
	require.NoError(t, err)
	require.Contains(t, msg, "value3")

	msg, err = ic.Imc.Get([]string{"key@0"})
	require.NoError(t, err)
	require.Contains(t, msg, "value3")

	msg, err = ic.Imc.Get([]string{"key@-0"})
	require.NoError(t, err)
	require.Contains(t, msg, "value3")

	msg, err = ic.Imc.Get([]string{"key@-1"})
	require.NoError(t, err)
	require.Contains(t, msg, "value2")

	msg, err = ic.Imc.Get([]string{"key@-2"})
	require.NoError(t, err)
	require.Contains(t, msg, "value1")

	_, err = ic.Imc.Get([]string{"key@notarevision"})
	require.Error(t, err)
}
