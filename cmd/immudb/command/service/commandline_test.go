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

package service

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/spf13/cobra"
)

func TestCommandline_Register(t *testing.T) {
	c := commandline{}
	cmd := c.Register(&cobra.Command{})
	assert.IsType(t, &cobra.Command{}, cmd)
}

func TestNewCommandLine(t *testing.T) {
	cml := NewCommandLine()
	assert.IsType(t, &commandline{}, cml)
}
