/*
Copyright 2019-2020 vChain, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

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
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
)

// NewCmd creates a new immutest command
func NewCmd(
	newImmuClient func(*client.Options) (client.ImmuClient, error),
	pwr c.PasswordReader,
	tr c.TerminalReader,
	ts client.TokenService,
	onError func(err error)) *cobra.Command {
	cmd := &cobra.Command{}
	Init(cmd,
		&commandline{
			newImmuClient: newImmuClient,
			pwr:           pwr,
			tr:            tr,
			tkns:          ts,
			onError:       onError,
			config:        c.Config{Name: "immutest"},
		})
	cmd.AddCommand(version.VersionCmd())
	return cmd
}
