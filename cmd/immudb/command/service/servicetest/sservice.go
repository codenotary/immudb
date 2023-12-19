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

package servicetest

import (
	"github.com/spf13/cobra"
	"github.com/takama/daemon"
)

func NewSservicemock() *Sservicemock {
	ss := &Sservicemock{}
	ss.NewDaemonF = func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
		return NewDaemonMock(), nil
	}

	ss.IsAdminF = func() (bool, error) {
		return true, nil
	}
	ss.InstallSetupF = func(serviceName string, cmd *cobra.Command) (err error) {
		return nil
	}
	ss.UninstallSetupF = func(serviceName string) (err error) {
		return nil
	}
	ss.EraseDataF = func(serviceName string) (err error) {
		return nil
	}
	ss.IsRunningF = func(status string) bool {
		return false
	}
	ss.GetDefaultConfigPathF = func(serviceName string) (string, error) {
		return "", nil
	}

	ss.ReadConfigF = func(serviceName string) (err error) {
		return nil
	}
	ss.InstallConfigF = func(serviceName string) (err error) {
		return nil
	}
	ss.CopyExecInOsDefaultF = func(execPath string) (newExecPath string, err error) {
		return "", nil
	}
	ss.GetDefaultExecPathF = func(serviceName string) (string, error) {
		return "", nil
	}
	ss.UninstallExecutablesF = func(serviceName string) error {
		return nil
	}
	return ss
}

type Sservicemock struct {
	NewDaemonF            func(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error)
	IsAdminF              func() (bool, error)
	InstallSetupF         func(serviceName string, cmd *cobra.Command) (err error)
	UninstallSetupF       func(serviceName string) (err error)
	EraseDataF            func(serviceName string) (err error)
	IsRunningF            func(status string) bool
	GetDefaultConfigPathF func(serviceName string) (string, error)
	ReadConfigF           func(serviceName string) (err error)
	InstallConfigF        func(serviceName string) (err error)
	CopyExecInOsDefaultF  func(execPath string) (newExecPath string, err error)
	GetDefaultExecPathF   func(serviceName string) (string, error)
	UninstallExecutablesF func(serviceName string) error
}

func (ss *Sservicemock) NewDaemon(name, description string, dependencies ...string) (d daemon.Daemon, err error) {
	return ss.NewDaemonF(name, description, dependencies...)
}

func (ss *Sservicemock) IsAdmin() (bool, error) {
	return ss.IsAdminF()
}
func (ss *Sservicemock) InstallSetup(serviceName string, cmd *cobra.Command) (err error) {
	return ss.InstallSetupF(serviceName, cmd)
}
func (ss *Sservicemock) UninstallSetup(serviceName string) (err error) {
	return ss.UninstallSetupF(serviceName)
}
func (ss *Sservicemock) EraseData(serviceName string) (err error) {
	return ss.EraseDataF(serviceName)
}
func (ss *Sservicemock) IsRunning(status string) bool {
	return ss.IsRunningF(status)
}
func (ss *Sservicemock) GetDefaultConfigPath(serviceName string) (string, error) {
	return ss.GetDefaultConfigPathF(serviceName)
}

func (ss *Sservicemock) ReadConfig(serviceName string) (err error) {
	return ss.ReadConfigF(serviceName)
}
func (ss *Sservicemock) InstallConfig(serviceName string) (err error) {
	return ss.InstallConfigF(serviceName)
}
func (ss *Sservicemock) CopyExecInOsDefault(execPath string) (newExecPath string, err error) {
	return ss.CopyExecInOsDefaultF(execPath)
}
func (ss *Sservicemock) GetDefaultExecPath(serviceName string) (string, error) {
	return ss.CopyExecInOsDefaultF(serviceName)
}
func (ss *Sservicemock) UninstallExecutables(serviceName string) error {
	return ss.UninstallExecutablesF(serviceName)
}

type SservicePermissionsMock struct{}

func (ssp SservicePermissionsMock) GroupCreateIfNotExists() (err error) {
	return nil
}
func (ssp SservicePermissionsMock) UserCreateIfNotExists() (err error) {
	return nil
}
func (ssp SservicePermissionsMock) SetOwnership(path string) (err error) {
	return nil
}
