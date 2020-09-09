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
