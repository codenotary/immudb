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

package service

import (
	"os"

	"github.com/codenotary/immudb/cmd/helper"
	srvc "github.com/codenotary/immudb/cmd/immuadmin/command/service/configs"
	service "github.com/codenotary/immudb/cmd/immuadmin/command/service/constants"
	immusrvc "github.com/codenotary/immudb/pkg/service"
	"github.com/spf13/cobra"
)

func NewCommandLine() *commandline {
	op := immusrvc.Option{
		ExecPath:      service.ExecPath,
		ConfigPath:    service.ConfigPath,
		ManPath:       service.ManPath,
		User:          service.OSUser,
		Group:         service.OSGroup,
		StartUpConfig: service.StartUpConfig,
		UsageDetails:  service.UsageDet,
		UsageExamples: service.UsageExamples,
		Config: map[string][]byte{
			"immudb": srvc.ConfigImmudb,
			"immugw": srvc.ConfigImmugw,
		},
	}
	s := immusrvc.NewSService(&op)
	t := helper.NewTerminalReader(os.Stdin)
	c := helper.Config{Name: "immuadmin"}
	return &commandline{c, s, t}
}

type Commandline interface {
	Service(cmd *cobra.Command)
}

type commandline struct {
	config   helper.Config
	sservice immusrvc.Sservice
	treader  helper.TerminalReader
}

func (cld *commandline) Register(rootCmd *cobra.Command) *cobra.Command {
	cld.Service(rootCmd)
	return rootCmd
}
