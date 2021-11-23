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

package sessions

import (
	"fmt"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	m := NewManager(nil)
	require.IsType(t, new(manager), m)
}

func TestSessionGuard(t *testing.T) {
	m := NewManager(nil)
	go func() {
		err := m.StartSessionsGuard()
		require.NoError(t, err)
	}()
	time.Sleep(time.Second * 1)
	err := m.StopSessionsGuard()
	require.NoError(t, err)
}

type firms struct {
	sync.Mutex
	firms map[string]string
}

func (f *firms) set(s string) {
	f.Lock()
	defer f.Unlock()
	f.firms[s] = s
}

func TestCallback(t *testing.T) {

	f := &firms{
		firms: make(map[string]string),
	}

	m := NewManager(nil)
	go func() {
		err := m.StartSessionsGuard()
		require.NoError(t, err)
	}()

	rand.Seed(time.Now().UnixNano())
	counter := 0
	for i := 1; i <= 100; i++ {
		sessID := m.NewSession(&auth.User{
			Username: fmt.Sprintf("%d", i),
		}, 0)
		for j := 1; j <= 100; j++ {
			sess := m.GetSession(sessID).NewTransaction(true)
			counter++
			name := fmt.Sprintf("callback-%d", counter)
			sess.AddOnDeleteCallback(name, func() error {
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
				f.set(name)
				return nil
			})
		}
	}
	err := m.StopSessionsGuard()
	require.NoError(t, err)
	require.Equal(t, 10000, len(f.firms))
}
