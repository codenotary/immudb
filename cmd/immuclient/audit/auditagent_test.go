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

/*
import (
	"os"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/immudb/command/service/servicetest"
	"github.com/spf13/cobra"

	srvc "github.com/codenotary/immudb/cmd/immuclient/service/configs"
	"github.com/codenotary/immudb/cmd/immuclient/service/constants"
	immusrvc "github.com/codenotary/immudb/cmd/sservice"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func TestManageNotRoot(t *testing.T) {
	srvoptions := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(srvoptions)
	bs.Start()
defer bs.Stop()

	os.Setenv("audit-agent-interval", "1s")
	pidPath := "pid_path"

	ad := new(auditAgent)
	ad.firstRun = true
	op := immusrvc.Option{
		ExecPath:      constants.ExecPath,
		ConfigPath:    constants.ConfigPath,
		User:          constants.OSUser,
		Group:         constants.OSGroup,
		StartUpConfig: constants.StartUpConfig,
		UsageDetails:  constants.UsageDet,
		UsageExamples: constants.UsageExamples,
		Config:        srvc.ConfigImmuClient,
	}
	ad.service = immusrvc.NewSService(&op)

	logfilename := "logfile"
	logfile, err := os.OpenFile(logfilename, os.O_APPEND, 0755)
	require.NoError(t, err)
	ad.logfile = logfile
	ad.logger = logger.NewSimpleLogger("immuclientd", logfile)

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ad.opts = options().WithMetrics(false).WithDialOptions(dialOptions).WithMTLs(false).WithPidPath(pidPath)
	_, err = ad.InitAgent()
	require.NoError(t, err, "InitAgent")
	defer func() { os.RemoveAll(pidPath); os.RemoveAll(logfilename) }()

	_, err = ad.Manage([]string{"uninstall"}, &cobra.Command{})
	if err == nil || !strings.Contains(err.Error(), "You must have root user privileges. Possibly using 'sudo' command should help") {
		t.Fatal("Manage fail, expected error")
	}

	_, err = ad.Manage([]string{"start"}, &cobra.Command{})
	if err == nil || !strings.Contains(err.Error(), "You must have root user privileges. Possibly using 'sudo' command should help") {
		t.Fatal("Manage fail, expected error")
	}

	_, err = ad.Manage([]string{"restart"}, &cobra.Command{})
	if err == nil || !strings.Contains(err.Error(), "You must have root user privileges. Possibly using 'sudo' command should help") {
		t.Fatal("Manage fail, expected error")
	}

	_, err = ad.Manage([]string{"stop"}, &cobra.Command{})
	if err == nil || !strings.Contains(err.Error(), "You must have root user privileges. Possibly using 'sudo' command should help") {
		t.Fatal("Manage fail, expected error")
	}

	_, err = ad.Manage([]string{"status"}, &cobra.Command{})
	if err == nil || !strings.Contains(err.Error(), "You must have root user privileges. Possibly using 'sudo' command should help") {
		t.Fatal("Manage fail, expected error")
	}
}

func TestManage(t *testing.T) {
	srvoptions := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(srvoptions)
	bs.Start()
defer bs.Stop()

	os.Setenv("audit-agent-interval", "1s")
	pidPath := "pid_path_2"

	ad := new(auditAgent)
	ad.firstRun = true

	ad.service = servicetest.Sservicemock{}

	logfilename := "logfile"
	logfile, err := os.OpenFile(logfilename, os.O_APPEND, 0755)
	require.NoError(t, err)
	ad.logfile = logfile
	ad.logger = logger.NewSimpleLogger("immuclientd", logfile)

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ad.opts = options().WithMetrics(false).WithDialOptions(dialOptions).WithMTLs(false).WithPidPath(pidPath)
	_, err = ad.InitAgent()
	require.NoError(t, err, "InitAgent")
	os.RemoveAll(pidPath)
	defer func() { os.RemoveAll(pidPath); os.RemoveAll(logfilename) }()

	_, err = ad.Manage([]string{}, &cobra.Command{})
	require.NoError(t, err, "Manage start audit fail")
	os.RemoveAll(pidPath)

	_, err = ad.Manage([]string{"install"}, &cobra.Command{})
	require.NoError(t, err, "Manage install audit fail")
	os.RemoveAll(pidPath)

	_, err = ad.Manage([]string{"uninstall"}, &cobra.Command{})
	require.NoError(t, err, "Manage uninstall fail")
	os.RemoveAll(pidPath)

	_, err = ad.Manage([]string{"start"}, &cobra.Command{})
	require.NoError(t, err, "Manage start fail")
	os.RemoveAll(pidPath)

	_, err = ad.Manage([]string{"restart"}, &cobra.Command{})
	require.NoError(t, err, "Manage restart fail")
	os.RemoveAll(pidPath)

	_, err = ad.Manage([]string{"stop"}, &cobra.Command{})
	require.NoError(t, err, "Manage restart")
	os.RemoveAll(pidPath)

	_, err = ad.Manage([]string{"status"}, &cobra.Command{})
	require.NoError(t, err, "Manage status")

}

func TestOptions(t *testing.T) {
	defer viper.Reset()

	viper.Set("immudb-port", "30000")
	viper.Set("immudb-address", "127.0.0.1")
	viper.Set("tokenfile", "tokenfile")
	viper.Set("mtls", true)
	viper.Set("certificate", "cert")
	viper.Set("servername", "myservername")
	viper.Set("pkey", "pkey")
	viper.Set("clientcas", "clientcas")
	viper.Set("pidfile", "pidfilename")
	viper.Set("logfile", "logfilename")
	op := options()
	if op.Address != "127.0.0.1" ||
		op.Port != 30000 ||
		op.TokenFileName != "tokenfile" ||
		!op.MTLs ||
		op.MTLsOptions.Certificate != "cert" ||
		op.MTLsOptions.ClientCAs != "clientcas" ||
		op.MTLsOptions.Pkey != "pkey" ||
		op.MTLsOptions.Servername != "myservername" ||
		op.PidPath != "pidfilename" ||
		op.LogFileName != "logfilename" {
		t.Fatal("Options fail")
	}
}
*/
