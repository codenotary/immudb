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

package immudb

import (
	"fmt"
	"testing"

	"github.com/codenotary/immudb/cmd/immudb/command/service/servicetest"
	"github.com/stretchr/testify/require"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/cobra"
)

func TestNewCmd(t *testing.T) {
	cl := Commandline{}
	cmd, err := cl.NewRootCmd(server.DefaultServer())
	require.NoError(t, err)
	require.IsType(t, &cobra.Command{}, cmd)
}

func TestNewCmdInitializeError(t *testing.T) {
	cl := Commandline{}
	s := servicetest.NewDefaultImmuServerMock()
	s.InitializeF = func() error {
		return fmt.Errorf("error")
	}
	cmd, err := cl.NewRootCmd(s)
	require.NoError(t, err)
	err = cmd.Execute()
	require.Error(t, err)
}
