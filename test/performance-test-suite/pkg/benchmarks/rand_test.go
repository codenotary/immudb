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

package benchmarks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRandStringGen(t *testing.T) {
	rsd := NewRandStringGen(16)
	defer rsd.Stop()

	for i := 0; i < 100; i++ {
		b := rsd.GetRnd()
		require.Len(t, b, 16)
		require.Regexp(t, "^[0-9a-f]+$", string(b))
	}
}
