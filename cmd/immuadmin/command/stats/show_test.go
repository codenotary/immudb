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

package stats

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/immuadmin/command/stats/statstest"
	"github.com/stretchr/testify/require"
)

func TestShowMetricsRaw(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.Write(statstest.StatsResponse)
	}))
	defer testServer.Close()
	var sw strings.Builder
	require.NoError(t, ShowMetricsRaw(&sw, testServer.URL))
}

func TestShowMetricsAsText(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.Write(statstest.StatsResponse)
	}))
	defer testServer.Close()
	var sw strings.Builder
	require.NoError(t, ShowMetricsAsText(&sw, testServer.URL))
}
