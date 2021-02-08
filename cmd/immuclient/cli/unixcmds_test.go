/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompleter(t *testing.T) {
	cli := new(cli)
	cli.commands = make(map[string]*command)
	cli.commandsList = make([]*command, 0)
	cli.initCommands()
	cm := cli.completer("safe")

	assert.EqualValues(t, 4, len(cm))
}

func TestClear(t *testing.T) {
	oses := []string{"linux", "windows", "darwin"}
	for _, os := range oses {
		clearcmd := clear[os]
		assert.IsType(t, func() {}, clearcmd)
		clearcmd()
	}
}
