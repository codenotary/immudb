/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitCommands(t *testing.T) {
	t.SkipNow()

	cli := new(cli)
	cli.commands = make(map[string]*command)
	cli.commandsList = make([]*command, 0)
	cli.initCommands()
	assert.EqualValues(t, 25, len(cli.commands))
}
