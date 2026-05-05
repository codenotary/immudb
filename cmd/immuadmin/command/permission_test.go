/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

package immuadmin

import (
	"strings"
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// TestPermissionFromString locks the CLI's permission-string parsing so the
// `sysadmin` option (added for issue #2052 to let operators bootstrap a
// second sysadmin from immuadmin) cannot silently regress and so the error
// message keeps listing every accepted value.
func TestPermissionFromString(t *testing.T) {
	cases := []struct {
		in   string
		want uint32
	}{
		{"read", auth.PermissionR},
		{"readwrite", auth.PermissionRW},
		{"admin", auth.PermissionAdmin},
		{"sysadmin", auth.PermissionSysAdmin},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := permissionFromString(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	t.Run("unknown value rejected with all options listed", func(t *testing.T) {
		_, err := permissionFromString("god")
		require.Error(t, err)
		msg := err.Error()
		for _, opt := range []string{"read", "readwrite", "admin", "sysadmin"} {
			require.True(t, strings.Contains(msg, opt),
				"error message %q must mention %q", msg, opt)
		}
	})
}
