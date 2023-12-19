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

package audit

import (
	"fmt"
	"time"
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
	go e.Run()
}

func (e *executable) Stop() {
	e.stop <- struct{}{}
}

func (e *executable) Run() {
	fmt.Println(time.Duration(e.a.cycleFrequency) * time.Second)
	e.a.ImmuAudit.Run(time.Duration(e.a.cycleFrequency)*time.Second, false, e.stop, e.stop)
}
