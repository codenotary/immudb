package statisticscmd

import (
	"fmt"
	"time"

	ui "github.com/gizak/termui/v3"
)

/**

Implement terminal-based controller

xterm color reference https://jonasjacek.github.io/colors/
*/

func loadAndRender(loader MetricsLoader, cntrl Controller) error {
	metricsFamilies, err := loader.Load()
	if err != nil {
		return err
	}
	cntrl.Render(metricsFamilies)
	return nil
}

func runUI(loader MetricsLoader, memStats bool) error {
	if err := ui.Init(); err != nil {
		return fmt.Errorf("failed to initialize termui: %v", err)
	}
	defer ui.Close()

	var controller Controller
	if !memStats {
		controller = newDBStatsController()
	} else {
		controller = newMemStatsController()
	}
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
