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

	"github.com/stretchr/testify/require"
)

func TestHealthCheck(t *testing.T) {
	cli := setupTest(t)

	msg, err := cli.healthCheck([]string{})
	require.NoError(t, err, "HealthCheck fail")
	require.Contains(t, msg, "Health check OK", "HealthCheck fail")
}

func TestHistory(t *testing.T) {
	cli := setupTest(t)

	msg, err := cli.history([]string{"key"})
	require.NoError(t, err, "History fail")
	require.Contains(t, msg, "key not found", "History fail")

	_, err = cli.set([]string{"key", "value"})
	require.NoError(t, err, "History fail")

	msg, err = cli.history([]string{"key"})
	require.NoError(t, err, "History fail")
	require.Contains(t, msg, "value", "History fail")
}

func TestVersion(t *testing.T) {
	cli := setupTest(t)

	msg, err := cli.version([]string{"key"})
	require.NoError(t, err, "version fail")
	require.Contains(t, msg, "no version info available", "version fail")
}
