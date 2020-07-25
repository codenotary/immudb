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
	"os"
	"strings"
	"testing"

	srvc "github.com/codenotary/immudb/cmd/immuclient/service/configs"
	service "github.com/codenotary/immudb/cmd/immuclient/service/constants"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	immusrvc "github.com/codenotary/immudb/pkg/service"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func TestManage(t *testing.T) {
	srvoptions := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(srvoptions)
	bs.Start()

	os.Setenv("audit-agent-interval", "1s")
	pidPath := "pid_path"
	viper.Set("pidfile", pidPath)

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

	logfilename := "logfile"
	logfile, err := os.OpenFile(logfilename, os.O_APPEND, 0755)
	if err != nil {
		logfile = os.Stderr
	}
	ad.logfile = logfile
	ad.logger = logger.NewSimpleLogger("immuclientd", logfile)

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	ad.opts = options().WithMetrics(false).WithDialOptions(&dialOptions).WithMTLs(false)
	_, err = ad.InitAgent()
	if err != nil {
		t.Fatal("InitAgent", err)
	}
	defer func() { os.RemoveAll(pidPath); os.RemoveAll(logfilename) }()

	// _, err = ad.Manage([]string{"install"})
	// if err == nil || !strings.Contains(err.Error(), "You must have root user privileges. Possibly using 'sudo' command should help") {
	// 	t.Fatal("Manage fail, expected error")
	// }

	_, err = ad.Manage([]string{"uninstall"})
	if err == nil || !strings.Contains(err.Error(), "You must have root user privileges. Possibly using 'sudo' command should help") {
		t.Fatal("Manage fail, expected error")
	}

	_, err = ad.Manage([]string{"start"})
	if err == nil || !strings.Contains(err.Error(), "You must have root user privileges. Possibly using 'sudo' command should help") {
		t.Fatal("Manage fail, expected error")
	}

	_, err = ad.Manage([]string{"restart"})
	if err == nil || !strings.Contains(err.Error(), "You must have root user privileges. Possibly using 'sudo' command should help") {
		t.Fatal("Manage fail, expected error")
	}

	_, err = ad.Manage([]string{"stop"})
	if err == nil || !strings.Contains(err.Error(), "You must have root user privileges. Possibly using 'sudo' command should help") {
		t.Fatal("Manage fail, expected error")
	}

	_, err = ad.Manage([]string{"status"})
	if err == nil || !strings.Contains(err.Error(), "You must have root user privileges. Possibly using 'sudo' command should help") {
		t.Fatal("Manage fail, expected error")
	}

}

func TestOptions(t *testing.T) {
	viper.Set("immudb-port", "30000")
	viper.Set("immudb-address", "127.0.0.1")
	viper.Set("tokenfile", "tokenfile")
	viper.Set("mtls", true)
	viper.Set("certificate", "cert")
	viper.Set("servername", "myservername")
	viper.Set("pkey", "pkey")
	viper.Set("clientcas", "clientcas")
	op := options()
	if op.Address != "127.0.0.1" ||
		op.Port != 30000 ||
		op.TokenFileName != "tokenfile" ||
		!op.MTLs ||
		op.MTLsOptions.Certificate != "cert" ||
		op.MTLsOptions.ClientCAs != "clientcas" ||
		op.MTLsOptions.Pkey != "pkey" ||
		op.MTLsOptions.Servername != "myservername" {
		t.Fatal("Options fail")
	}
}
