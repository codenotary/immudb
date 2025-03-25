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

package immuc_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHistory(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.History([]string{"key"})
	require.NoError(t, err, "History fail")
	require.Contains(t, msg, "key not found", "History fail")

	_, err = ic.Imc.Set([]string{"key", "value"})
	require.NoError(t, err, "History fail")
	msg, err = ic.Imc.History([]string{"key"})
	require.NoError(t, err, "History fail")
	require.Contains(t, msg, "value", "History fail")
}
