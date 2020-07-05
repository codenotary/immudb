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

type immuGwServerMock struct {
	running []bool
	sync.RWMutex
}

func (igm *immuGwServerMock) Start() error {
	defer igm.Unlock()
	igm.running = append(igm.running, true)
	return nil
}
func (igm *immuGwServerMock) Stop() error {
	defer igm.Unlock()
	igm.running = append(igm.running, false)
	return nil
}

func TestService(t *testing.T) {
	s := new(immuGwServerMock)
	service := Service{s}

	s.Lock()
	service.Start()
	s.Lock()
	service.Stop()
	s.Lock()
	service.Run()

	require.Eventually(
		t,
		func() bool {
			return len(s.running) == 3 &&
				s.running[0] == true && s.running[1] == false && s.running[2] == true
		},
		1*time.Second,
		100*time.Millisecond,
		"expected [true, false, true], actual %v",
		s.running,
	)
}
