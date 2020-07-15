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

// Detached launch command in background
func Detached() {
	var err error
	var executable string
	var args []string

	if executable, err = os.Executable(); err != nil {
		QuitToStdErr(err)
	}

	for i, k := range os.Args {
		if k != "--"+DetachedFlag && k != "-"+DetachedShortFlag && i != 0 {
			args = append(args, k)
		}
	}

	cmd := exec.Command(executable, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err = cmd.Start(); err != nil {
		QuitToStdErr(err)
	}
	time.Sleep(1 * time.Second)
	fmt.Fprintf(
		os.Stdout, "%s%s has been started with %sPID %d%s\n",
		Green, filepath.Base(executable), Blue, cmd.Process.Pid, Reset)
	os.Exit(0)
}
