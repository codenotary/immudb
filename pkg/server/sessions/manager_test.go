/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	m, err := NewManager(DefaultOptions())
	require.NoError(t, err)
	require.IsType(t, new(manager), m)
}

func TestSessionGuard(t *testing.T) {
	m, err := NewManager(DefaultOptions())
	require.NoError(t, err)
	go func() {
		err := m.StartSessionsGuard()
		require.NoError(t, err)
	}()
	time.Sleep(time.Second * 1)
	err = m.StopSessionsGuard()
	require.NoError(t, err)
}

const SGUARD_CHECK_INTERVAL = time.Millisecond * 250
const MAX_SESSION_INACTIVE = time.Millisecond * 2000
const MAX_SESSION_AGE = time.Millisecond * 3000
const TIMEOUT = time.Millisecond * 1000
const KEEPSTATUS = time.Millisecond * 300
const WORK_TIME = time.Millisecond * 1000

func TestManager_ExpireSessions(t *testing.T) {
	const SESS_NUMBER = 60
	const KEEP_ACTIVE = 20
	const KEEP_INFINITE = 20

	sessOptions := &Options{
		SessionGuardCheckInterval: SGUARD_CHECK_INTERVAL,
		MaxSessionInactivityTime:  MAX_SESSION_INACTIVE,
		MaxSessionAgeTime:         MAX_SESSION_AGE,
		Timeout:                   TIMEOUT,
	}
	m, err := NewManager(sessOptions)
	require.NoError(t, err)

	m.logger = logger.NewSimpleLogger("immudb session guard", os.Stdout) //.CloneWithLevel(logger.LogDebug)
	go func(mng *manager) {
		err := mng.StartSessionsGuard()
		require.NoError(t, err)
	}(m)

	rand.Seed(time.Now().UnixNano())

	sessIDs := make(chan string, SESS_NUMBER)

	wg := sync.WaitGroup{}
	for i := 1; i <= SESS_NUMBER; i++ {
		wg.Add(1)
		go func(u int, cs chan string, w *sync.WaitGroup) {
			lid, err := m.NewSession(&auth.User{
				Username: fmt.Sprintf("%d", u),
			}, nil)
			if err != nil {
				t.Error(err)
			}
			cs <- lid.GetID()
			w.Done()
		}(i, sessIDs, &wg)
	}

	wg.Wait()
	require.Equal(t, SESS_NUMBER, m.SessionCount())

	activeDone := make(chan bool)
	infiniteDone := make(chan bool)
	// keep active
	for ac := 0; ac < KEEP_ACTIVE; ac++ {
		go keepActive(<-sessIDs, m, activeDone)
	}
	for alc := 0; alc < KEEP_INFINITE; alc++ {
		go keepActive(<-sessIDs, m, infiniteDone)
	}

	fInactiveC := 0
	fActiveC := 0
	time.Sleep(WORK_TIME)
	for _, s := range m.sessions {
		switch s.GetStatus() {
		case active:
			fActiveC++
		case inactive:
			fInactiveC++
		}
	}

	require.Equal(t, SESS_NUMBER, fActiveC)
	require.Equal(t, 0, fInactiveC)

	activeDone <- true

	time.Sleep(MAX_SESSION_AGE + TIMEOUT)
	fInactiveC = 0
	fActiveC = 0

	for _, s := range m.sessions {
		switch s.GetStatus() {
		case active:
			fActiveC++
		case inactive:
			fInactiveC++
		}
	}

	require.Equal(t, 0, fActiveC)
	require.Equal(t, 0, fInactiveC)

	err = m.StopSessionsGuard()
	require.NoError(t, err)
}

func keepActive(id string, m *manager, done chan bool) {
	t := time.NewTicker(KEEPSTATUS)
	for {
		select {
		case <-t.C:
			m.UpdateSessionActivityTime(id)
		case <-done:
			return
		}
	}
}
