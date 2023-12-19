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
	"fmt"
	"time"

	ui "github.com/gizak/termui/v3"
)

// Statsui ...
type Statsui interface {
}

type statsui struct {
	cntrl  Controller
	Loader MetricsLoader
	Tui    Tui
}

func (s statsui) loadAndRender() error {
	ms, err := s.Loader.Load()
	if err != nil {
		return err
	}
	s.cntrl.Render(ms)
	return nil
}

func (s statsui) runUI(singleRun bool) error {
	if err := s.Tui.Init(); err != nil {
		return fmt.Errorf("failed to initialize termui: %v", err)
	}
	defer s.Tui.Close()

	ms, err := s.Loader.Load()
	if err != nil {
		return err
	}
	s.cntrl = newStatsController(ms.isHistogramsDataAvailable(), s.Tui)
	if err := s.loadAndRender(); err != nil {
		return err
	}

	ev := s.Tui.PollEvents()

	ticker := time.NewTicker(requestTimeout)
	defer ticker.Stop()
	tick := ticker.C

	for {
		select {
		case e := <-ev:
			switch e.Type {
			case ui.KeyboardEvent:
				switch e.ID {
				case "q", "<C-c>", "<Escape>":
					return nil
				}
			case ui.ResizeEvent:
				s.cntrl.Resize()
			}
		case <-tick:
			if err := s.loadAndRender(); err != nil {
				return err
			}
			if singleRun {
				return nil
			}
		}
	}
}
