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

package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/test/bufconn"
)

func TestService(t *testing.T) {
	bufSize := 1024 * 1024
	l := bufconn.Listen(bufSize)

	dir := t.TempDir()

	options := DefaultOptions().
		WithDir(dir).
		WithAuth(true).
		WithListener(l).
		WithPort(22222).
		WithMetricsServer(false)

	server := DefaultServer().WithOptions(options).(*ImmuServer)

	err := server.Initialize()
	require.NoError(t, err)
	srvc := &Service{
		ImmuServerIf: server,
	}

	srvc.Start()
	time.Sleep(1 * time.Second)
	srvc.Stop()
}
