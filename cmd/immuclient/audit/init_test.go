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

package audit

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	args := []string{"help"}
	err := Init(args, &cobra.Command{})
	assert.NoError(t, err)
}

func TestInitWrongArg(t *testing.T) {
	args := []string{"wrong"}
	err := Init(args, &cobra.Command{})
	assert.Error(t, err)
}
