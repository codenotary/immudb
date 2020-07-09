package servicetest

import "github.com/takama/daemon"

type daemonmock struct {
	daemon.Daemon
}

// SetTemplate - sets service config template
func (d daemonmock) SetTemplate(string) error {
	return nil
}

// Install the service into the system
func (d daemonmock) Install(args ...string) (string, error) {
	return "", nil
}

// Remove the service and all corresponding files from the system
func (d daemonmock) Remove() (string, error) {
	return "", nil
}

// Start the service
func (d daemonmock) Start() (string, error) {
	return "", nil
}

// Stop the service
func (d daemonmock) Stop() (string, error) {
	return "", nil
}

// Status - check the service status
func (d daemonmock) Status() (string, error) {
	return "", nil
}

// Run - run executable service
func (d daemonmock) Run(e daemon.Executable) (string, error) {
	return "", nil
}
