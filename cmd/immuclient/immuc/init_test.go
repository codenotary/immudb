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

package immuc_test

import (
	"testing"

	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/stretchr/testify/require"

	. "github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConnect(t *testing.T) {
	options := server.DefaultOptions().WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	opts := OptionsFromEnv()
	opts.GetImmudbClientOptions().
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
		}).WithDir(t.TempDir())
	imc, err := Init(opts)
	require.NoError(t, err)
	err = imc.Connect([]string{""})
	require.NoError(t, err)
	imc.WithFileTokenService(tokenservice.NewInmemoryTokenService())
}
