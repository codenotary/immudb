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
	"io"
	"os"

	"github.com/codenotary/immudb/pkg/server"
	immusrvc "github.com/codenotary/immudb/pkg/service"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/auditor"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/spf13/viper"
	"github.com/takama/daemon"
)

// AuditAgent ...
type AuditAgent interface {
	Manage(args []string) (string, error)
	InitAgent() (AuditAgent, error)
}

type auditAgent struct {
	service        immusrvc.Sservice
	Daemon         daemon.Daemon
	cycleFrequency int
	metrics        prometheusMetrics
	ImmuAudit      auditor.Auditor
	immuc          client.ImmuClient
	firstRun       bool
	opts           *client.Options
	logger         logger.Logger
	Pid            server.PIDFile
	logfile        io.Writer
}

func (a *auditAgent) Manage(args []string) (string, error) {
	var err error
	var msg string
	var localFile string
	var execPath string
	var command string
	if len(args) > 0 {
		command = args[0]
	}
	exec := newExecutable(a)

	if command == "install" {
		if _, err = a.InitAgent(); err != nil {
			QuitToStdErr(err.Error())
		}
		localFile = viper.GetString("local-file")
		if localFile, err = a.service.GetExecutable(localFile, name); err != nil {
			return "", err
		}
	}

	execPath, err = a.service.GetDefaultExecPath(localFile)

	if stringInSlice("--remove-files", os.Args) {
		localFile = viper.GetString("local-file")
	}

	if command == "install" {
		if execPath, err = a.service.CopyExecInOsDefault(localFile); err != nil {
			return "", err
		}
	}

	// todo remove all involved files
	if remove := viper.GetBool("remove-files"); remove {
		if err = os.Remove(execPath); err != nil {
			return "", err
		}
		return "", nil
	}

	a.Daemon, err = a.service.NewDaemon(name, name, execPath)
	if err != nil {
		return "", err
	}
	if len(command) > 0 {
		switch command {
		case "install":
			fmt.Println("installing " + localFile + "...")
			if err = a.service.InstallSetup(name); err != nil {
				return "", err
			}
			logfile, err := os.OpenFile(viper.GetString("logfile"), os.O_APPEND, 0755)
			if err != nil {
				logfile = os.Stderr
			}
			a.logfile = logfile
			a.logger = logger.NewSimpleLogger("immuclientd", logfile)
			fmt.Println("installing " + localFile + "...")
			configpath, err := a.service.GetDefaultConfigPath(name)
			if err != nil {
				return "", err
			}
			if msg, err = a.Daemon.Install("audit-mode", "--config", configpath); err != nil {
				return "", err
			}
			fmt.Println(msg)

			if msg, err = a.Daemon.Start(); err != nil {
				return "", err
			}
			return msg, nil
		case "uninstall":
			var status string
			if status, err = a.Daemon.Status(); err != nil {
				if err == daemon.ErrNotInstalled {
					return "", err
				}
			}
			// stopping service first
			if a.service.IsRunning(status) {
				if msg, err = a.Daemon.Stop(); err != nil {
					return "", err
				}
				fmt.Println(msg)
			}
			return a.Daemon.Remove()
		case "start":
			if msg, err = a.Daemon.Start(); err != nil {
				return "", err
			}
			return msg, nil
		case "restart":
			if msg, err = a.Daemon.Stop(); err != nil {
				return "", err
			}
			fmt.Println(msg)
			if msg, err = a.Daemon.Start(); err != nil {
				return "", err
			}
			return msg, nil
		case "stop":
			return a.Daemon.Stop()
		case "status":
			return a.Daemon.Status()
		default:
			return fmt.Sprintf("Invalid arg %s", command), nil
		}
	}
	logfile, err := os.OpenFile(viper.GetString("logfile"), os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		logfile = os.Stderr
	}
	a.logfile = logfile
	a.logger = logger.NewSimpleLogger("immuclientd", logfile)
	if _, err := a.InitAgent(); err != nil {
		return "", err
	}
	return a.Daemon.Run(exec)
}

func options() *client.Options {
	port := viper.GetInt("immudb-port")
	address := viper.GetString("immudb-address")
	tokenFileName := viper.GetString("tokenfile")
	mtls := viper.GetBool("mtls")
	certificate := viper.GetString("certificate")
	servername := viper.GetString("servername")
	pkey := viper.GetString("pkey")
	clientcas := viper.GetString("clientcas")
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithTokenFileName(tokenFileName).
		WithMTLs(mtls)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = client.DefaultMTLsOptions().
			WithServername(servername).
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(clientcas)
	}
	return options
}
