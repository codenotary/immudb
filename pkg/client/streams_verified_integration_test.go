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

package client

import (
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/codenotary/immudb/pkg/streamutils"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestImmuServer_StreamVerifiedSetAndGet(t *testing.T) {
	cliIF, ctx := externalImmudbClient(t)

	tmpFile, err := streamtest.GenerateDummyFile("myFile1", (8<<20)-1)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile.Name())
	require.NoError(t, err)

	txhdr, err := cliIF.StreamVerifiedSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, txhdr)

	entry, err := cliIF.StreamVerifiedGet(ctx, &schema.VerifiableGetRequest{
		KeyRequest: &schema.KeyRequest{Key: []byte(tmpFile.Name())},
	})
	require.NoError(t, err)
	require.Equal(t, (8<<20)-1, len(entry.Value))
}
