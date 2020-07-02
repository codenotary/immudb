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

package server

import (
	"os"
	"testing"
)

func TestPid(t *testing.T) {
	pidFilename := "pid_file"
	_, err := NewPid("./")
	if err == nil {
		t.Errorf("PID failed test of ./ filename ")
	}
	_, err = NewPid("")
	if err == nil {
		t.Errorf("PID failed test of empty filename ")
	}
	pid, err := NewPid(pidFilename)
	if err != nil {
		t.Errorf("Error creating pid file %v", err)
	}
	defer pid.Remove()
	_, err = os.Stat(pidFilename)
	if os.IsNotExist(err) {
		t.Errorf("pid file not created")
	}
}
