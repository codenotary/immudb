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
