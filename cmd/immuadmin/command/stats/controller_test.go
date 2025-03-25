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
	"bytes"
	"testing"

	"github.com/codenotary/immudb/cmd/immuadmin/command/stats/statstest"
	ui "github.com/gizak/termui/v3"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
)

type tuiMock struct{}

func (t tuiMock) TerminalDimensions() (int, int) {
	return 1024, 768
}
func (t tuiMock) Render(items ...ui.Drawable) {}

func (t tuiMock) Init() error {
	return nil
}
func (t tuiMock) Close() {}

func (t tuiMock) PollEvents() <-chan ui.Event {
	ch := make(chan ui.Event)
	return ch
}

func TestNewStatsController(t *testing.T) {
	c := newStatsController(true, tuiMock{})
	assert.IsType(t, &statsController{}, c)
}

func TestRender(t *testing.T) {
	c := newStatsController(true, tuiMock{})

	textParser := expfmt.TextParser{}

	metricsFamilies, _ := textParser.TextToMetricFamilies(bytes.NewReader(statstest.StatsResponse))
	ms := &metrics{}
	ms.populateFrom(&metricsFamilies)
	c.Render(ms)
	assert.IsType(t, &statsController{}, c)
}
