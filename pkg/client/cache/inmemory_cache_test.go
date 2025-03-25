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

package cache

import (
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestInMemoryCache(t *testing.T) {
	imc := NewInMemoryCache()
	require.IsType(t, &inMemoryCache{}, imc)

	err := imc.Set("server1", "db11", &schema.ImmutableState{TxId: 11, TxHash: []byte{11}})
	require.NoError(t, err)
	err = imc.Set("server1", "db12", &schema.ImmutableState{TxId: 12, TxHash: []byte{12}})
	require.NoError(t, err)
	err = imc.Set("server2", "db21", &schema.ImmutableState{TxId: 21, TxHash: []byte{21}})
	require.NoError(t, err)

	root, err := imc.Get("server1", "db11")
	require.NoError(t, err)
	require.Equal(t, uint64(11), root.GetTxId())
	require.Equal(t, []byte{11}, root.GetTxHash())

	root, err = imc.Get("server1", "db12")
	require.NoError(t, err)
	require.Equal(t, uint64(12), root.GetTxId())
	require.Equal(t, []byte{12}, root.GetTxHash())

	root, err = imc.Get("server2", "db21")
	require.NoError(t, err)
	require.Equal(t, uint64(21), root.GetTxId())
	require.Equal(t, []byte{21}, root.GetTxHash())

	_, err = imc.Get("unknownServer", "db11")
	require.ErrorContains(t, err, "no roots found for server")
	_, err = imc.Get("server1", "unknownDb")
	require.ErrorContains(t, err, "no state found for server")

	err = imc.Lock("server1")
	require.ErrorIs(t, err, ErrNotImplemented)

	err = imc.Unlock()
	require.ErrorIs(t, err, ErrNotImplemented)

	err = imc.ServerIdentityCheck("server1", "identity1")
	require.NoError(t, err)

	err = imc.ServerIdentityCheck("server1", "identity2")
	require.ErrorIs(t, err, ErrServerIdentityValidationFailed)

	err = imc.ServerIdentityCheck("server1", "identity1")
	require.NoError(t, err)
}
