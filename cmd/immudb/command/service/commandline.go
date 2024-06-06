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

package service

import (
	"os"

	"github.com/codenotary/immudb/cmd/immudb/command/service/config"
	"github.com/codenotary/immudb/cmd/immudb/command/service/constants"
	"github.com/codenotary/immudb/cmd/sservice"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/spf13/cobra"
)

func NewCommandLine() *commandline {
	op := sservice.Option{
		ExecPath:      constants.ExecPath,
		ConfigPath:    constants.ConfigPath,
		User:          constants.OSUser,
		Group:         constants.OSGroup,
		StartUpConfig: constants.StartUpConfig,
		UsageDetails:  constants.UsageDet,
		UsageExamples: constants.UsageExamples,
		Config:        config.ConfigImmudb,
	}

	s := sservice.NewSService(&op)
	t := helper.NewTerminalReader(os.Stdin)
	c := helper.Config{Name: "immuadmin"}
	return &commandline{c, s, t}
}

type commandline struct {
	config   helper.Config
	sservice sservice.Sservice
	treader  helper.TerminalReader
}

/*type sserviceImmudb struct {
	sservice.Sservice
}*/

// NewSService ...
/*func NewImmudbSService(options *sservice.Option) sservice.Sservice {
	mps := immudb.NewManpageService()
	return sserviceImmudb{ sservice.Sservice{immuos.NewStandardOS(), viper.New(), mps, *options}}
}*/

type Commandline interface {
	Service(cmd *cobra.Command)
}

func (cld *commandline) Register(rootCmd *cobra.Command) *cobra.Command {
	cld.Service(rootCmd)
	return rootCmd
}
