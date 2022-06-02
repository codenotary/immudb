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
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestNewManager(t *testing.T) {
	m, err := NewManager(DefaultOptions())
	require.NoError(t, err)
	require.IsType(t, new(manager), m)
	require.NotNil(t, m.sessions)
}

func TestSessionGuard(t *testing.T) {
	m, err := NewManager(DefaultOptions())
	require.NoError(t, err)

	err = m.StartSessionsGuard()
	require.NoError(t, err)

	time.Sleep(time.Second * 1)
	err = m.StopSessionsGuard()
	require.NoError(t, err)
}

func TestManager_ExpireSessions(t *testing.T) {
	const (
		SESS_NUMBER   = 60
		KEEP_ACTIVE   = 20
		KEEP_INFINITE = 20

		SGUARD_CHECK_INTERVAL = time.Millisecond * 2
		MAX_SESSION_INACTIVE  = time.Millisecond * 20
		MAX_SESSION_AGE       = time.Millisecond * 30
		TIMEOUT               = time.Millisecond * 10
		KEEPSTATUS            = time.Millisecond * 3
		WORK_TIME             = time.Millisecond * 10
	)

	sessOptions := &Options{
		SessionGuardCheckInterval: SGUARD_CHECK_INTERVAL,
		MaxSessionInactivityTime:  MAX_SESSION_INACTIVE,
		MaxSessionAgeTime:         MAX_SESSION_AGE,
		Timeout:                   TIMEOUT,
	}
	m, err := NewManager(sessOptions)
	require.NoError(t, err)

	m.logger = logger.NewSimpleLogger("immudb session guard", os.Stdout) //.CloneWithLevel(logger.LogDebug)
	err = m.StartSessionsGuard()
	require.NoError(t, err)

	rand.Seed(time.Now().UnixNano())

	sessIDs := make(chan string, SESS_NUMBER)
	sessErrs := make(chan error, SESS_NUMBER)

	wg := sync.WaitGroup{}
	for i := 1; i <= SESS_NUMBER; i++ {
		wg.Add(1)
		go func(u int) {
			defer wg.Done()

			lid, err := m.NewSession(&auth.User{
				Username: fmt.Sprintf("%d", u),
			}, nil)
			if err != nil {
				sessErrs <- err
			} else {
				sessIDs <- lid.GetID()
			}
		}(i)
	}
	wg.Wait()

	close(sessErrs)
	for err := range sessErrs {
		require.NoError(t, err)
	}

	require.Equal(t, SESS_NUMBER, m.SessionCount())

	activeDone := make(chan bool)
	infiniteDone := make(chan bool)
	// keep active

	wg = sync.WaitGroup{}
	for ac := 0; ac < KEEP_ACTIVE; ac++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keepActive(<-sessIDs, m, KEEPSTATUS, activeDone)
		}()
	}
	for alc := 0; alc < KEEP_INFINITE; alc++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keepActive(<-sessIDs, m, KEEPSTATUS, infiniteDone)
		}()
	}

	time.Sleep(WORK_TIME)

	fActiveC, fInactiveC := countSessions(m)

	require.Equal(t, SESS_NUMBER, fActiveC)
	require.Equal(t, 0, fInactiveC)

	close(activeDone)

	time.Sleep(MAX_SESSION_AGE + TIMEOUT)
	fActiveC, fInactiveC = countSessions(m)

	require.Equal(t, 0, fActiveC)
	require.Equal(t, 0, fInactiveC)

	err = m.StopSessionsGuard()
	require.NoError(t, err)

	close(infiniteDone)

	wg.Wait()
}

func keepActive(id string, m *manager, updateTime time.Duration, done chan bool) {
	t := time.NewTicker(updateTime)
	for {
		select {
		case <-t.C:
			m.UpdateSessionActivityTime(id)
		case <-done:
			t.Stop()
			return
		}
	}
}

func countSessions(m *manager) (fActiveC, fInactiveC int) {
	m.sessionMux.RLock()
	defer m.sessionMux.RUnlock()

	fActiveC, fInactiveC = 0, 0
	for _, s := range m.sessions {
		switch s.GetStatus() {
		case active:
			fActiveC++
		case inactive:
			fInactiveC++
		}
	}

	return fActiveC, fInactiveC
}
