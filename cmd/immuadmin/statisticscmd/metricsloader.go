/*
Copyright 2019-2020 vChain, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package statisticscmd

import (
	"fmt"
	"io/ioutil"
	"net/http"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func newMetricsLoader(url string) *metricsLoader {
	return &metricsLoader{
		url:    url,
		client: newHttpClient(),
	}
}

type metricsLoader struct {
	url    string
	client *http.Client
}

func (ml *metricsLoader) Load() (*map[string]*dto.MetricFamily, error) {
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
	return &metricsFamilies, nil
}
