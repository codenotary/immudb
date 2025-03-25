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

func TestReference(t *testing.T) {
	ic := setupTest(t)

	_, _ = ic.Imc.Set([]string{"key", "val"})

	msg, err := ic.Imc.SetReference([]string{"val", "key"})
	require.NoError(t, err, "Reference fail")
	require.Contains(t, msg, "value", "Reference failed")
}

func TestVerifiedSetReference(t *testing.T) {
	t.SkipNow()

	ic := setupTest(t)

	_, _ = ic.Imc.Set([]string{"key", "val"})

	msg, err := ic.Imc.VerifiedSetReference([]string{"val", "key"})
	require.NoError(t, err, "SafeReference fail")
	require.Contains(t, msg, "hash", "SafeReference failed")
}
