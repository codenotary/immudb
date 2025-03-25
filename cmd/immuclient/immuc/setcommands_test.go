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

func TestSet(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.Set([]string{"key", "val"})

	require.NoError(t, err, "Set fail")
	require.Contains(t, msg, "value", "Set failed")
}

func TestVerifiedSet(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.VerifiedSet([]string{"key", "val"})

	require.NoError(t, err, "VerifiedSet fail")
	require.Contains(t, msg, "value", "VerifiedSet failed")
}

func TestZAdd(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.VerifiedSet([]string{"key", "val"})
	require.NoError(t, err)

	msg, err := ic.Imc.ZAdd([]string{"val", "1", "key"})

	require.NoError(t, err, "ZAdd fail")
	require.Contains(t, msg, "hash", "ZAdd failed")
}

func _TestVerifiedZAdd(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.VerifiedSet([]string{"key", "val"})
	require.NoError(t, err)

	msg, err := ic.Imc.VerifiedZAdd([]string{"val", "1", "key"})

	require.NoError(t, err, "VerifiedZAdd fail")
	require.Contains(t, msg, "hash", "VerifiedZAdd failed")
}

func TestCreateDatabase(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.CreateDatabase([]string{"newdb"})
	require.NoError(t, err, "CreateDatabase fail")
	require.Contains(t, msg, "database successfully created", "CreateDatabase failed")

	_, err = ic.Imc.DatabaseList([]string{})
	require.NoError(t, err, "DatabaseList fail")

	msg, err = ic.Imc.UseDatabase([]string{"newdb"})
	require.NoError(t, err, "UseDatabase fail")
	require.Contains(t, msg, "newdb", "UseDatabase failed")
}
