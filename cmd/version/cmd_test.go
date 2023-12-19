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

package version

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	cmd := VersionCmd()

	// no version info
	collector := new(cmdtest.StdOutCollector)
	require.NoError(t, collector.Start())
	require.NoError(t, cmd.Execute())
	noVersionOutput, err := collector.Stop()
	require.NoError(t, err)
	require.Equal(t, "no version info available\n", noVersionOutput)

	// full version info
	App = "Some App"
	Version = "v1.0.0"
	Commit = "2F20B9ADF24C82A6AFEE0CEBF53B46A512FE9526"
	BuiltBy = "some.user@somedomain.com"
	builtAt, _ := time.Parse(time.RFC3339, "2020-07-13T23:28:09Z")
	BuiltAt = fmt.Sprintf("%d", builtAt.Unix())
	Static = "static"

	builtAtUnix, _ := strconv.ParseInt(BuiltAt, 10, 64)
	builtAtStr := time.Unix(builtAtUnix, 0).Format(time.RFC1123)
	expectedVersionOutput := strings.Join(
		[]string{
			"Some App v1.0.0",
			"Commit  : 2F20B9ADF24C82A6AFEE0CEBF53B46A512FE9526",
			"Built by: some.user@somedomain.com",
			"Built at: " + builtAtStr,
			"Static  : true\n",
		},
		"\n")
	require.NoError(t, collector.Start())
	require.NoError(t, cmd.Execute())
	versionOutput, err := collector.Stop()
	require.NoError(t, err)
	require.Equal(t, expectedVersionOutput, versionOutput)
}
