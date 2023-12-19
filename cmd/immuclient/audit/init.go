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

package audit

import (
	"fmt"

	"github.com/spf13/cobra"

	srvc "github.com/codenotary/immudb/cmd/immuclient/service/configs"
	service "github.com/codenotary/immudb/cmd/immuclient/service/constants"
	immusrvc "github.com/codenotary/immudb/cmd/sservice"
)

// Init ...
func Init(args []string, cmd *cobra.Command) (err error) {
	var auditAgent AuditAgent
	validargs := []string{"start", "install", "uninstall", "restart", "stop", "status", "help"}
	if len(args) > 0 && !stringInSlice(args[0], validargs) {
		return fmt.Errorf("ERROR: %v is not matching with any valid arguments.\n Available list is %v \n", args[0], validargs)
	}
	if auditAgent, err = NewAuditAgent(); err != nil {
		return err
	}
	if msg, err := auditAgent.Manage(args, cmd); err != nil {
		return err
	} else {
		fmt.Println(msg)
	}
	return nil
}

// NewAuditAgent ...
func NewAuditAgent() (AuditAgent, error) {
	ad := new(auditAgent)
	ad.firstRun = true
	op := immusrvc.Option{
		ExecPath:      service.ExecPath,
		ConfigPath:    service.ConfigPath,
		User:          service.OSUser,
		Group:         service.OSGroup,
		StartUpConfig: service.StartUpConfig,
		UsageDetails:  service.UsageDet,
		UsageExamples: service.UsageExamples,
		Config:        srvc.ConfigImmuClient,
	}
	ad.service = immusrvc.NewSService(&op)
	var err error
	ad.opts = options()
	daemon, err := ad.service.NewDaemon(name, description, name)
	if err != nil {
		return nil, err
	}
	ad.Daemon = daemon
	return ad, nil
}
