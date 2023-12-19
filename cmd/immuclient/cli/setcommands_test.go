/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	cli := setupTest(t)

	msg, err := cli.set([]string{"key", "val"})
	require.NoError(t, err, "Set fail")
	require.Contains(t, msg, "value", "Set failed")
}

func TestSafeSet(t *testing.T) {
	cli := setupTest(t)

	msg, err := cli.safeset([]string{"key", "val"})
	require.NoError(t, err, "SafeSet fail")
	require.Contains(t, msg, "value", "SafeSet failed")
}

func TestZAdd(t *testing.T) {
	cli := setupTest(t)

	_, err := cli.safeset([]string{"key", "val"})
	require.NoError(t, err)

	msg, err := cli.zAdd([]string{"val", "1", "key"})

	require.NoError(t, err, "ZAdd fail")
	require.Contains(t, msg, "hash", "ZAdd failed")
}

func TestSafeZAdd(t *testing.T) {
	cli := setupTest(t)

	_, err := cli.safeset([]string{"key", "val"})
	require.NoError(t, err)

	msg, err := cli.safeZAdd([]string{"val", "1", "key"})

	require.NoError(t, err, "SafeZAdd fail")
	require.Contains(t, msg, "hash", "SafeZAdd failed")
}
