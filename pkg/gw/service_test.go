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

package gw

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestService(t *testing.T) {
	var running []bool
	lock := new(sync.RWMutex)
	s := &ImmuGwServerMock{
		StartF: func() error {
			defer lock.Unlock()
			running = append(running, true)
			return nil
		},
		StopF: func() error {
			defer lock.Unlock()
			running = append(running, false)
			return nil
		},
	}
	service := Service{s}

	lock.Lock()
	service.Start()
	lock.Lock()
	service.Stop()
	lock.Lock()
	service.Run()

	require.Eventually(
		t,
		func() bool {
			return len(running) == 3 &&
				running[0] == true && running[1] == false && running[2] == true
		},
		1*time.Second,
		100*time.Millisecond,
		"expected [true, false, true], actual %v",
		running,
	)
}
