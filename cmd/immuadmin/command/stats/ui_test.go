/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package stats

import (
	"bytes"
	"testing"

	"github.com/codenotary/immudb/cmd/immuadmin/command/stats/statstest"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
)

func TestRunUI(t *testing.T) {
	sui := statsui{Loader: metricsLoaderMock{}, Tui: tuiMock{}}
	err := sui.runUI(true)
	assert.Nil(t, err)
}

type metricsLoaderMock struct{}

func (ml metricsLoaderMock) Load() (*metrics, error) {
	textParser := expfmt.TextParser{}
	metricsFamilies, _ := textParser.TextToMetricFamilies(bytes.NewReader(statstest.StatsResponse))
	ms := &metrics{}
	ms.populateFrom(&metricsFamilies)
	return ms, nil
}
