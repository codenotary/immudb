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

package audit

import (
	"fmt"
	"os"

	srvc "github.com/codenotary/immudb/cmd/immuclient/service/configs"
	service "github.com/codenotary/immudb/cmd/immuclient/service/constants"
	immusrvc "github.com/codenotary/immudb/pkg/service"
)

// Init ...
func Init(args []string) {
	IsAuditD(args)
	auditAgent, err := NewAuditAgent()
	if err != nil {
		QuitToStdErr(err.Error())
		return
	}
	msg, err := auditAgent.Manage(args)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(0)
	}
	fmt.Println(msg)
	os.Exit(0)
}

// NewAuditAgent ...
func NewAuditAgent() (AuditAgent, error) {
	ad := new(auditAgent)
	ad.firstRun = true
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
			"immuclient": srvc.ConfigImmuClient,
		},
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

// IsAuditD ...
func IsAuditD(args []string) {
	if len(args) == 0 {
		return
	}
	if args[0] == "" {
		return
	}
	m := "help"
	validargs := []string{"start", "install", "uninstall", "restart", "stop", "status", "help"}
	for i := range args {
		if args[i] == m {
			fmt.Println(service.UsageExamples)
			os.Exit(0)
		}
		for j := range validargs {
			if args[0] == validargs[j] {
				return
			}
		}
	}
	fmt.Printf("ERROR: %v is not matching with any valid arguments.\n Available list is %v \n", args[0], validargs)
	os.Exit(0)
}
