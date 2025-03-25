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

package immuclient

import (
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/stretchr/testify/require"

	"github.com/spf13/cobra"
)

func _TestInit(t *testing.T) {
	cm := NewCommand()
	require.Len(t, cm.Commands(), 28, "fail immuclient commands, wrong number of expected commands")
}

func TestConnect(t *testing.T) {
	ic := setupTest(t)

	cmd := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	_ = cmd.connect(&cobra.Command{}, []string{})
	cmd.disconnect(&cobra.Command{}, []string{})
}
