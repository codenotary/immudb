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
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/codenotary/immudb/pkg/server"

	"github.com/codenotary/immudb/cmd/immuclient/service"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/auditor"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/spf13/viper"
	"github.com/takama/daemon"
)

type AuditAgent interface {
	Manage(args []string) (string, error)
	InitAgent() (AuditAgent, error)
}

type auditAgent struct {
	daemon.Daemon
	cycleFrequency int
	promot         promotheusExporter
	ImmuAudit      auditor.Auditor
	immuc          client.ImmuClient
	rootStorage    io.Writer
	firstRun       bool
	opts           *client.Options
	logger         logger.Logger
	Pid            server.PIDFile
	logfile        io.Writer
}

func (a *auditAgent) writeRoot(root *schema.Root, t string) error {
	m := make(map[string]interface{}, 2)
	m["root"] = root
	m["timeStamp"] = t
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	data = append(data, []byte("\n")...)
	_, err = a.rootStorage.Write(data)
	if err != nil {
		return err
	}
	return nil
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
		_, err := a.InitAgent()
		if err != nil {
			QuitToStdErr(err.Error())
		}
		localFile = viper.GetString("local-file")
		if localFile, err = service.GetExecutable(localFile, name); err != nil {
			return "", err
		}
	}

	execPath = service.GetDefaultExecPath(localFile)

	if stringInSlice("--remove-files", os.Args) {
		localFile = viper.GetString("local-file")
	}

	if command == "install" {
		if execPath, err = service.CopyExecInOsDefault(localFile); err != nil {
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

	a.Daemon, err = service.NewDaemon(name, name, execPath)
	if err != nil {
		return "", err
	}
	if len(command) > 0 {
		switch command {
		case "install":
			fmt.Println("installing " + localFile + "...")
			if err = service.InstallSetup(name); err != nil {
				return "", err
			}
			logfile, err := os.OpenFile(viper.GetString("logfile"), os.O_APPEND, 0755)
			if err != nil {
				logfile = os.Stderr
			}
			a.logfile = logfile
			a.logger = logger.NewSimpleLogger("immuclientd", logfile)
			fmt.Println("installing " + localFile + "...")

			if msg, err = a.Install("audit-mode", "--config", service.GetDefaultConfigPath(name)); err != nil {
				return "", err
			}
			fmt.Println(msg)

			if msg, err = a.Start(); err != nil {
				return "", err
			}
			return msg, nil
		case "uninstall":
			var status string
			if status, err = a.Status(); err != nil {
				if err == daemon.ErrNotInstalled {
					return "", err
				}
			}
			// stopping service first
			if service.IsRunning(status) {
				if msg, err = a.Stop(); err != nil {
					return "", err
				}
				fmt.Println(msg)
			}
			return a.Remove()
		case "start":
			if msg, err = a.Start(); err != nil {
				return "", err
			}
			return msg, nil
		case "restart":
			if msg, err = a.Stop(); err != nil {
				return "", err
			}
			fmt.Println(msg)
			if msg, err = a.Start(); err != nil {
				return "", err
			}
			return msg, nil
		case "stop":
			return a.Stop()
		case "status":
			return a.Status()
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
	return a.Run(exec)
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

func getCommands() string {
	cmds := []string{"install", "uninstall", "start", "stop", "status", "restart"}
	for i := range os.Args {
		for j := range cmds {
			if os.Args[i] == cmds[j] {
				return cmds[j]
			}
		}
	}
	return ""
}
