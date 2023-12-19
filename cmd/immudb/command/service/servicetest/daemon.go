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

import "github.com/takama/daemon"

func NewDaemonMock() *daemonmock {
	dm := &daemonmock{}
	dm.SetTemplateF = func(string) error {
		return nil
	}
	dm.InstallF = func(args ...string) (string, error) {
		return "", nil
	}
	dm.RemoveF = func() (string, error) {
		return "", nil
	}
	dm.StartF = func() (string, error) {
		return "", nil
	}
	dm.StopF = func() (string, error) {
		return "", nil
	}
	dm.StatusF = func() (string, error) {
		return "", nil
	}
	dm.RunF = func(e daemon.Executable) (string, error) {
		return "", nil
	}
	return dm
}

type daemonmock struct {
	daemon.Daemon
	SetTemplateF func(string) error
	InstallF     func(args ...string) (string, error)
	RemoveF      func() (string, error)
	StartF       func() (string, error)
	StopF        func() (string, error)
	StatusF      func() (string, error)
	RunF         func(e daemon.Executable) (string, error)
}

func (d *daemonmock) SetTemplate(t string) error {
	return d.SetTemplateF(t)
}

func (d *daemonmock) Install(args ...string) (string, error) {
	return d.InstallF(args...)
}

func (d *daemonmock) Remove() (string, error) {
	return d.RemoveF()
}

func (d *daemonmock) Start() (string, error) {
	return d.StartF()
}

func (d *daemonmock) Stop() (string, error) {
	return d.StopF()
}

func (d *daemonmock) Status() (string, error) {
	return d.StatusF()
}

func (d *daemonmock) Run(e daemon.Executable) (string, error) {
	return d.RunF(e)
}
