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

package stats

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/prometheus/common/expfmt"
)

// MetricsLoader ...
type MetricsLoader interface {
	Load() (*metrics, error)
}

func newMetricsLoader(url string) MetricsLoader {
	return &metricsLoader{
		url:    url,
		client: newHTTPClient(),
	}
}

type metricsLoader struct {
	url    string
	client *http.Client
}

func (ml *metricsLoader) Load() (*metrics, error) {
	resp, err := ml.client.Get(ml.url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET %s returned unexpected HTTP Status %d with body %s", ml.url, resp.StatusCode, string(body))
	}
	textParser := expfmt.TextParser{}
	metricsFamilies, err := textParser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, err
	}
	ms := &metrics{}
	ms.populateFrom(&metricsFamilies)
	return ms, nil
}
