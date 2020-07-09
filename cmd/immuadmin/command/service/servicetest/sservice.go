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

package servicetest

import (
	"github.com/takama/daemon"
)

type Sservicemock struct{}

func (ss Sservicemock) NewDaemon(name, description, execStartPath string, dependencies ...string) (d daemon.Daemon, err error) {
	return daemonmock{}, nil
}
func (ss Sservicemock) IsAdmin() (bool, error) {
	return true, nil
}
func (ss Sservicemock) InstallSetup(serviceName string) (err error) {
	return nil
}
func (ss Sservicemock) UninstallSetup(serviceName string) (err error) {
	return nil
}
func (ss Sservicemock) InstallConfig(serviceName string) (err error) {
	return nil
}
func (ss Sservicemock) RemoveProgramFiles(serviceName string) (err error) {
	return nil
}
func (ss Sservicemock) EraseData(serviceName string) (err error) {
	return nil
}
func (ss Sservicemock) GetExecutable(input string, serviceName string) (exec string, err error) {
	return "", nil
}
func (ss Sservicemock) CopyExecInOsDefault(execPath string) (newExecPath string, err error) {
	return "", nil
}
func (ss Sservicemock) GetDefaultExecPath(localFile string) (string, error) {
	return "", nil
}
func (ss Sservicemock) GetDefaultConfigPath(serviceName string) (string, error) {
	return "", nil
}
func (ss Sservicemock) IsRunning(status string) bool {
	return true
}
func (ss Sservicemock) ReadConfig(serviceName string) (err error) {
	return nil
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
