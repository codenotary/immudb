/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immudb/command/service/servicetest"
	"github.com/stretchr/testify/require"
	"github.com/takama/daemon"

	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestCommandLine_ServiceImmudbInstall(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.NewSservicemock(), tr}

	cld.Service(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"service", "install"})
	err := cmd.Execute()
	assert.NoError(t, err)
}

func TestCommandLine_ServiceImmudbUninstallAbortUnintall(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"n"}
	cld := commandline{helper.Config{}, servicetest.NewSservicemock(), tr}

	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "uninstall"})
	err := cmd.Execute()
	assert.NoError(t, err)
}

func TestCommandLine_ServiceImmudbUninstallRemovingData(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"y", "y"}
	cld := commandline{helper.Config{}, servicetest.NewSservicemock(), tr}

	cld.Service(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"service", "uninstall"})
	err := cmd.Execute()
	assert.NoError(t, err)
	out, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	assert.Contains(t, string(out), "uninstall")
}

func TestCommandLine_ServiceImmudbUninstallWithoutRemoveData(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"y", "n"}
	cld := commandline{helper.Config{}, servicetest.NewSservicemock(), tr}

	cld.Service(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"service", "uninstall"})
	err := cmd.Execute()
	assert.NoError(t, err)
	out, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	assert.Contains(t, string(out), "uninstall")
}

func TestCommandLine_ServiceImmudbStop(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.NewSservicemock(), tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "stop"})
	err := cmd.Execute()
	assert.NoError(t, err)
}

func TestCommandLine_ServiceImmudbStart(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.NewSservicemock(), tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "start"})
	err := cmd.Execute()
	assert.NoError(t, err)
}

func TestCommandLine_ServiceImmudbDelayed(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.NewSservicemock(), tr}
	cld.Service(cmd)
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"--time", "1"}
	cmd.SetArgs([]string{"service", "stop", "--time", "1"})
	err := cmd.Execute()
	assert.Error(t, err)
}

func TestCommandLine_ServiceImmudbDelayedInner(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.NewSservicemock(), tr}
	cld.Service(cmd)
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"--delayed", "1"}
	cmd.SetArgs([]string{"service", "stop", "--delayed", "1"})
	err := cmd.Execute()
	assert.NoError(t, err)
}

func TestCommandLine_ServiceImmudbExtraArgs(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.NewSservicemock(), tr}
	cld.Service(cmd)
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"--time", "1", "dummy"}
	cmd.SetArgs([]string{"service", "stop", "--time", "1", "dummy"})
	err := cmd.Execute()
	assert.Error(t, err)
}

func TestCommandLine_ServiceImmudbRestart(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.NewSservicemock(), tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "restart"})
	err := cmd.Execute()
	assert.NoError(t, err)
}

func TestCommandLine_ServiceImmudbStatus(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.NewSservicemock(), tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "status"})
	err := cmd.Execute()
	assert.NoError(t, err)
}

func TestCommandline_ServiceNewDaemonError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	ss := servicetest.NewSservicemock()
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		return nil, fmt.Errorf("error")
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "status"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceInstallSetupError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	ss := servicetest.NewSservicemock()
	ss.InstallSetupF = func(serviceName string, cmd *cobra.Command) (err error) {
		return fmt.Errorf("error")
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "install"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceGetDefaultConfigPathError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	ss := servicetest.NewSservicemock()
	ss.GetDefaultConfigPathF = func(serviceName string) (string, error) {
		return "", fmt.Errorf("error")
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "install"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceInstallError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}

	ss := servicetest.NewSservicemock()
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		dm := servicetest.NewDaemonMock()
		dm.InstallF = func(args ...string) (string, error) {
			return "", fmt.Errorf("error")
		}
		return dm, nil
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "install"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceInstallDaemonStartError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}

	ss := servicetest.NewSservicemock()
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		dm := servicetest.NewDaemonMock()
		dm.StartF = func() (string, error) {
			return "", fmt.Errorf("error")
		}
		return dm, nil
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "install"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceUninstallDaemonStatusError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}

	ss := servicetest.NewSservicemock()
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		dm := servicetest.NewDaemonMock()
		dm.StatusF = func() (string, error) {
			return "", daemon.ErrNotInstalled
		}
		return dm, nil
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "uninstall"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceUninstallIsRunning(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"y", "y"}

	ss := servicetest.NewSservicemock()
	ss.IsRunningF = func(status string) bool {
		return true
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "uninstall"})
	err := cmd.Execute()
	require.NoError(t, err)
}

func TestCommandline_ServiceUninstallEraseDataError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"y", "y"}

	ss := servicetest.NewSservicemock()
	ss.EraseDataF = func(serviceName string) error {
		return fmt.Errorf("error")
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "uninstall"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceUninstallNotWanted(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"n"}

	ss := servicetest.NewSservicemock()

	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "uninstall"})
	err := cmd.Execute()
	require.NoError(t, err)
}
func TestCommandline_ServiceUninstallTerminalError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.ReadFromTerminalYNF = func(string) (string, error) {
		return "", fmt.Errorf("error")
	}
	ss := servicetest.NewSservicemock()
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "uninstall"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceUninstallDaemonStopError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"y", "y"}

	ss := servicetest.NewSservicemock()
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		dm := servicetest.NewDaemonMock()
		dm.StopF = func() (string, error) {
			return "", fmt.Errorf("error")
		}
		return dm, nil
	}
	ss.IsRunningF = func(status string) bool {
		return true
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "uninstall"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceUninstallDaemonRemoveError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"y", "y"}

	ss := servicetest.NewSservicemock()
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		dm := servicetest.NewDaemonMock()
		dm.RemoveF = func() (string, error) {
			return "", fmt.Errorf("error")
		}
		return dm, nil
	}
	ss.IsRunningF = func(status string) bool {
		return true
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "uninstall"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceUninstallDaemonUninstallSetupError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"y", "y"}

	ss := servicetest.NewSservicemock()

	ss.UninstallSetupF = func(serviceName string) (err error) {
		return fmt.Errorf("error")
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "uninstall"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceStartDaemonStartError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}

	ss := servicetest.NewSservicemock()
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		dm := servicetest.NewDaemonMock()
		dm.StartF = func() (string, error) {
			return "", fmt.Errorf("error")
		}
		return dm, nil
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "start"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServiceStopDaemonStopError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}

	ss := servicetest.NewSservicemock()
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		dm := servicetest.NewDaemonMock()
		dm.StopF = func() (string, error) {
			return "", fmt.Errorf("error")
		}
		return dm, nil
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "stop"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServicRestartDaemonStopError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}

	ss := servicetest.NewSservicemock()
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		dm := servicetest.NewDaemonMock()
		dm.StopF = func() (string, error) {
			return "", fmt.Errorf("error")
		}
		return dm, nil
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "restart"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServicRestartDaemonStartError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}

	ss := servicetest.NewSservicemock()
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		dm := servicetest.NewDaemonMock()
		dm.StartF = func() (string, error) {
			return "", fmt.Errorf("error")
		}
		return dm, nil
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "restart"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServicRestarIsNotAdminError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}

	ss := servicetest.NewSservicemock()
	ss.IsAdminF = func() (bool, error) {
		return false, fmt.Errorf("error")
	}

	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "restart"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_ServicStatusDaemonStatusError(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}

	ss := servicetest.NewSservicemock()
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		dm := servicetest.NewDaemonMock()
		dm.StatusF = func() (string, error) {
			return "", fmt.Errorf("error")
		}
		return dm, nil
	}
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "status"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_CommandInvalidArgument(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	ss := servicetest.NewSservicemock()
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "wrong"})
	err := cmd.Execute()
	require.Error(t, err)
}

func TestCommandline_CommandMissingCommandName(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	ss := servicetest.NewSservicemock()
	cld := commandline{helper.Config{}, ss, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service"})
	err := cmd.Execute()
	require.Error(t, err)
}
