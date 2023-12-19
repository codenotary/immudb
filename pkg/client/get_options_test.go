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

package client_test

import (
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/stretchr/testify/require"
)

func TestGetOptionsAtTx(t *testing.T) {
	req := &schema.KeyRequest{Key: []byte("key")}
	err := client.AtTx(100)(req)
	require.NoError(t, err)
	require.Equal(t, &schema.KeyRequest{
		Key:  []byte("key"),
		AtTx: 100,
	}, req)
}

func TestGetOptionsSinceTx(t *testing.T) {
	req := &schema.KeyRequest{Key: []byte("key")}
	err := client.SinceTx(101)(req)
	require.NoError(t, err)
	require.Equal(t, &schema.KeyRequest{
		Key:     []byte("key"),
		SinceTx: 101,
	}, req)
}

func TestGetOptionsNoWait(t *testing.T) {
	req := &schema.KeyRequest{Key: []byte("key")}
	err := client.NoWait(true)(req)
	require.NoError(t, err)
	require.Equal(t, &schema.KeyRequest{
		Key:    []byte("key"),
		NoWait: true,
	}, req)
}

func TestGetOptionsAtRevision(t *testing.T) {
	req := &schema.KeyRequest{Key: []byte("key")}
	err := client.AtRevision(102)(req)
	require.NoError(t, err)
	require.Equal(t, &schema.KeyRequest{
		Key:        []byte("key"),
		AtRevision: 102,
	}, req)
}
