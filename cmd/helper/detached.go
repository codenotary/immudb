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

package helper

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// DetachedFlag ...
const DetachedFlag = "detached"

// DetachedShortFlag ...
const DetachedShortFlag = "d"

type Execs interface {
	Command(name string, arg ...string) *exec.Cmd
}

type execs struct{}

func (e execs) Command(name string, arg ...string) *exec.Cmd {
	return exec.Command(name, arg...)
}

type Plauncher interface {
	Detached() error
}

type plauncher struct {
	e Execs
}

func NewPlauncher() *plauncher {
	return &plauncher{execs{}}
}

// Detached launch command in background
func (pl plauncher) Detached() error {
	var err error
	var executable string
	var args []string

	if executable, err = os.Executable(); err != nil {
		return err
	}

	for i, k := range os.Args {
		if k != "--"+DetachedFlag && k != "-"+DetachedShortFlag && i != 0 {
			args = append(args, k)
		}
	}

	cmd := pl.e.Command(executable, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err = cmd.Start(); err != nil {
		return err
	}
	time.Sleep(1 * time.Second)
	fmt.Fprintf(
		os.Stdout, "%s%s has been started with %sPID %d%s\n",
		Green, filepath.Base(executable), Blue, cmd.Process.Pid, Reset)
	return nil
}
