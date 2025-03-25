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

package immutest

import (
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/spf13/cobra"
)

// NewCmd creates a new immutest command
func NewCmd(
	pwr c.PasswordReader,
	tr c.TerminalReader,
	ts tokenservice.TokenService,
	onError func(err error)) *cobra.Command {
	cmd := &cobra.Command{}
	Init(cmd,
		&commandline{
			pwr:     pwr,
			tr:      tr,
			tkns:    ts,
			onError: onError,
			config:  c.Config{Name: "immutest"},
		})
	cmd.AddCommand(version.VersionCmd())
	return cmd
}
