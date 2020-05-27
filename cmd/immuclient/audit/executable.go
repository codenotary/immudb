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

package audit

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

type executable struct {
	a    *auditAgent
	stop chan struct{}
}

func newExecutable(a *auditAgent) *executable {
	exec := new(executable)
	exec.a = a
	exec.stop = make(chan struct{}, 1)
	return exec
}

func (e *executable) Start() {
	tt := time.NewTicker(time.Duration(e.a.cycleFrequency) * time.Second)

	go func() {
		go func() {
			e.a.promot.port = viper.GetString("prometheus-port")
			e.a.promot.port = viper.GetString("prometheus-host")
			err := e.a.promot.startPromoExporter()
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
		for {
			select {
			case <-tt.C:
				e.a.ImmuTc.Run(time.Duration(e.a.cycleFrequency)*time.Second, e.stop, e.stop)
			case <-e.stop:
				return
			}

		}
	}()
}

func (e *executable) Stop() {
	e.stop <- struct{}{}
}

func (e *executable) Run() {
	go func() {
		e.a.promot.port = viper.GetString("prometheus-port")
		e.a.promot.address = viper.GetString("prometheus-host")
		err := e.a.promot.startPromoExporter()
		if err != nil {
			fmt.Println(err.Error())
		}
	}()
	e.a.ImmuTc.Run(time.Duration(e.a.cycleFrequency)*time.Second, e.stop, e.stop)
}
