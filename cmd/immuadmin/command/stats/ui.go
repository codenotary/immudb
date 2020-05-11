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

package stats

import (
	"fmt"
	"time"

	ui "github.com/gizak/termui/v3"
)

func loadAndRender(loader MetricsLoader, cntrl Controller) error {
	ms, err := loader.Load()
	if err != nil {
		return err
	}
	cntrl.Render(ms)
	return nil
}

func runUI(loader MetricsLoader) error {
	if err := ui.Init(); err != nil {
		return fmt.Errorf("failed to initialize termui: %v", err)
	}
	defer ui.Close()

	ms, err := loader.Load()
	if err != nil {
		return err
	}
	var controller = newStatsController(ms.isHistogramsDataAvailable())
	if err := loadAndRender(loader, controller); err != nil {
		return err
	}

	ev := ui.PollEvents()

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
				controller.Resize()
			}
		case <-tick:
			if err := loadAndRender(loader, controller); err != nil {
				return err
			}
		}
	}
}
