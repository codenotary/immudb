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
	"bytes"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type execsmock struct{}

func (e execsmock) Command(name string, arg ...string) *exec.Cmd {
	cmd := exec.Command("tr", "a-z", "A-Z")
	cmd.Stdin = strings.NewReader("some input")
	var out bytes.Buffer
	cmd.Stdout = &out
	return cmd
}

func TestDetached(t *testing.T) {
	pl := plauncher{execsmock{}}
	err := pl.Detached()
	assert.NoError(t, err)
}

func TestNewPlauncher(t *testing.T) {
	pl := NewPlauncher()
	assert.IsType(t, &plauncher{}, pl)
}

func TestExecs_Command(t *testing.T) {
	ex := execs{}
	c := ex.Command("tr", "a-z", "A-Z")
	assert.IsType(t, &exec.Cmd{}, c)
}
