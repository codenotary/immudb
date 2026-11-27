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

package fmessages

import (
	"testing"

	pgserrors "github.com/codenotary/immudb/pkg/pgsql/errors"
	"github.com/stretchr/testify/require"
)

func TestParseShortPayloadIsMalformed(t *testing.T) {
	_, err := ParsePasswordMsg(nil)
	require.ErrorIs(t, err, pgserrors.ErrMalformedMessage)

	_, err = ParseQueryMsg([]byte{})
	require.ErrorIs(t, err, pgserrors.ErrMalformedMessage)

	_, err = ParseDescribeMsg([]byte{})
	require.ErrorIs(t, err, pgserrors.ErrMalformedMessage)

	_, err = ParseDescribeMsg([]byte{'S'})
	require.ErrorIs(t, err, pgserrors.ErrMalformedMessage)

	d, err := ParseDescribeMsg([]byte{'S', 0})
	require.NoError(t, err)
	require.Equal(t, "S", d.DescType)
	require.Empty(t, d.Name)
}
