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
