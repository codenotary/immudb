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
	"github.com/codenotary/immudb/pkg/client/rootservice"
	"github.com/spf13/cobra"
	"os"

	c "github.com/codenotary/immudb/cmd/helper"
	immusrvc "github.com/codenotary/immudb/cmd/sservice"
	"github.com/codenotary/immudb/pkg/server"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/auditor"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/spf13/viper"
	"github.com/takama/daemon"
)

// AuditAgent ...
type AuditAgent interface {
	Manage(args []string, cmd *cobra.Command) (string, error)
	InitAgent() (AuditAgent, error)
}

type auditAgent struct {
	service        immusrvc.Sservice
	uuidProvider   rootservice.UuidProvider
	Daemon         daemon.Daemon
	cycleFrequency int
	metrics        prometheusMetrics
	ImmuAudit      auditor.Auditor
	immuc          client.ImmuClient
	firstRun       bool
	opts           *client.Options
	logger         logger.Logger
	Pid            server.PIDFile
	logfile        *os.File
}

func (a *auditAgent) Manage(args []string, cmd *cobra.Command) (string, error) {
	var err error
	var msg string
	var command string
	if len(args) > 0 {
		command = args[0]
	}
	exec := newExecutable(a)

	if command == "install" {
		if _, err = a.InitAgent(); err != nil {
			c.QuitToStdErr(err)
		}
	}

	a.Daemon, err = a.service.NewDaemon(name, name)
	if err != nil {
		return "", err
	}
	if len(command) > 0 {
		switch command {
		case "install":
			if err = a.service.InstallSetup(name, cmd); err != nil {
				return "", err
			}
			logfile, err := os.OpenFile(a.opts.LogFileName, os.O_APPEND, 0755)
			if err != nil {
				logfile = os.Stderr
			}
			a.logfile = logfile
			a.logger = logger.NewSimpleLogger("immuclientd", logfile)
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
			if msg, err = a.Daemon.Remove(); err != nil {
				return "", err
			}
			if err = a.service.UninstallSetup(name); err != nil {
				return "", err
			}
			return msg, nil

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

	a.logger = logger.NewSimpleLogger("immuclientd", os.Stdout)
	if a.opts.LogFileName != "" {
		a.logger, a.logfile, err = logger.NewFileLogger("immuclientd", a.opts.LogFileName)
		defer a.logfile.Close()
		if err != nil {
			return "", err
		}
	}
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
	pidpath := viper.GetString("pidfile")
	prometheusPort := viper.GetString("prometheus-port")
	prometheusHost := viper.GetString("prometheus-host")
	logfilename := viper.GetString("logfile")
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithTokenFileName(tokenFileName).
		WithMTLs(mtls).WithPidPath(pidpath).
		WithPrometheusPort(prometheusPort).
		WithPrometheusHost(prometheusHost).
		WithLogFileName(logfilename)
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
