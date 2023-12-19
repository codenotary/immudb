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

package sessions

import (
	"bytes"
	"fmt"
	"math/bits"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestNewManager(t *testing.T) {
	m, err := NewManager(DefaultOptions())
	require.NoError(t, err)
	require.IsType(t, new(manager), m)
	require.NotNil(t, m.sessions)
}

func TestNewManagerCornerCases(t *testing.T) {
	_, err := NewManager(nil)
	require.ErrorIs(t, err, ErrInvalidOptionsProvided)
}

func TestSessionGuard(t *testing.T) {
	m, err := NewManager(DefaultOptions())
	require.NoError(t, err)

	isRunning := m.IsRunning()
	require.False(t, isRunning)

	err = m.StartSessionsGuard()
	require.NoError(t, err)

	isRunning = m.IsRunning()
	require.True(t, isRunning)

	err = m.StartSessionsGuard()
	require.ErrorIs(t, err, ErrGuardAlreadyRunning)

	isRunning = m.IsRunning()
	require.True(t, isRunning)

	time.Sleep(time.Second * 1)

	isRunning = m.IsRunning()
	require.True(t, isRunning)

	err = m.StopSessionsGuard()
	require.NoError(t, err)

	isRunning = m.IsRunning()
	require.False(t, isRunning)

	err = m.StopSessionsGuard()
	require.ErrorIs(t, err, ErrGuardNotRunning)

	isRunning = m.IsRunning()
	require.False(t, isRunning)

	_, _, _, err = m.expireSessions(time.Now())
	require.ErrorIs(t, err, ErrGuardNotRunning)
}

func TestManagerMaxSessions(t *testing.T) {
	m, err := NewManager(DefaultOptions().WithMaxSessions(1))
	require.NoError(t, err)

	sess, err := m.NewSession(&auth.User{}, nil)
	require.NoError(t, err)

	sess2, err := m.NewSession(&auth.User{}, nil)
	require.ErrorIs(t, err, ErrMaxSessionsReached)
	require.Nil(t, sess2)

	err = m.DeleteSession(sess.id)
	require.NoError(t, err)
}

func TestGetSessionNotFound(t *testing.T) {
	m, err := NewManager(DefaultOptions())
	require.NoError(t, err)

	sess, err := m.GetSession("non-existing-session")
	require.ErrorIs(t, err, ErrSessionNotFound)
	require.Nil(t, sess)
}

func TestManager_ExpireSessions(t *testing.T) {
	const (
		SESS_NUMBER = 60
		SESS_ACTIVE = 30

		TICK = time.Millisecond

		SGUARD_CHECK_INTERVAL  = TICK * 2
		MAX_SESSION_INACTIVE   = TICK * 10
		TIMEOUT                = TICK * 50
		STATUS_UPDATE_INTERVAL = TICK * 1
	)

	sessOptions := DefaultOptions().
		WithSessionGuardCheckInterval(SGUARD_CHECK_INTERVAL).
		WithMaxSessionInactivityTime(MAX_SESSION_INACTIVE).
		WithMaxSessionAgeTime(infinity).
		WithTimeout(TIMEOUT)

	m, err := NewManager(sessOptions)
	require.NoError(t, err)

	m.logger = logger.NewSimpleLogger("immudb session guard", os.Stdout)

	sessIDs := make(chan string, SESS_NUMBER)

	t.Run("must correctly create sessions in parallel", func(t *testing.T) {
		wg := sync.WaitGroup{}
		for i := 1; i <= SESS_NUMBER; i++ {
			wg.Add(1)
			go func(u int) {
				defer wg.Done()

				lid, err := m.NewSession(&auth.User{
					Username: fmt.Sprintf("%d", u),
				}, nil)
				require.NoError(t, err)

				sessIDs <- lid.GetID()
			}(i)
		}
		wg.Wait()
		if t.Failed() {
			t.FailNow()
		}

		require.Equal(t, SESS_NUMBER, m.SessionCount())
	})

	t.Run("check if session guard removes sessions", func(t *testing.T) {
		err = m.StartSessionsGuard()
		require.NoError(t, err)

		// keep some sessions active
		keepActiveDone := make(chan bool)
		wg := sync.WaitGroup{}
		for ac := 0; ac < SESS_ACTIVE; ac++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				id := <-sessIDs

				t := time.NewTicker(STATUS_UPDATE_INTERVAL)
				for {
					select {
					case <-t.C:
						m.UpdateSessionActivityTime(id)
					case <-keepActiveDone:
						t.Stop()
						return
					}
				}
			}()
		}

		// Ensure session guard is doing its job
		time.Sleep(2 * TIMEOUT)
		require.Equal(t, SESS_ACTIVE, m.SessionCount())

		// Cleanup
		close(keepActiveDone)
		wg.Wait()

		err = m.StopSessionsGuard()
		require.NoError(t, err)
	})
}

func TestManagerSessionExpiration(t *testing.T) {

	m, err := NewManager(DefaultOptions().
		WithMaxSessionInactivityTime(5 * time.Second).
		WithTimeout(10 * time.Second).
		WithMaxSessionAgeTime(100 * time.Second),
	)
	require.NoError(t, err)

	m.logger = logger.NewSimpleLogger("immudb session guard", os.Stdout)
	err = m.StartSessionsGuard()
	require.NoError(t, err)

	nowTime := time.Now()

	t.Run("do not expire new sessions", func(t *testing.T) {
		sess, err := m.NewSession(&auth.User{}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, m.SessionCount())

		count, inactive, del, err := m.expireSessions(nowTime)
		require.NoError(t, err)
		require.Equal(t, 1, count)
		require.Zero(t, inactive)
		require.Zero(t, del)

		require.Equal(t, 1, m.SessionCount())

		m.DeleteSession(sess.id)
	})

	t.Run("do not expire inactive sessions before additional timeout", func(t *testing.T) {
		sess, err := m.NewSession(&auth.User{}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, m.SessionCount())

		sess.lastActivityTime = nowTime.Add(-7 * time.Second)

		count, inactive, del, err := m.expireSessions(nowTime)
		require.NoError(t, err)
		require.Equal(t, 1, count)
		require.Equal(t, 1, inactive)
		require.Zero(t, del)

		require.Equal(t, 1, m.SessionCount())

		m.DeleteSession(sess.id)
	})

	t.Run("expire inactive sessions once timeout passes", func(t *testing.T) {
		sess, err := m.NewSession(&auth.User{}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, m.SessionCount())

		sess.lastActivityTime = nowTime.Add(-13 * time.Second)

		count, inactive, del, err := m.expireSessions(nowTime)
		require.NoError(t, err)
		require.Zero(t, count)
		require.Zero(t, inactive)
		require.Equal(t, 1, del)

		require.Equal(t, 0, m.SessionCount())

		m.DeleteSession(sess.id)
	})

	t.Run("expire active sessions due to max age", func(t *testing.T) {
		sess, err := m.NewSession(&auth.User{}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, m.SessionCount())

		sess.lastActivityTime = nowTime
		sess.creationTime = nowTime.Add(-101 * time.Second)

		count, inactive, del, err := m.expireSessions(nowTime)
		require.NoError(t, err)
		require.Zero(t, count)
		require.Zero(t, inactive)
		require.Equal(t, 1, del)

		require.Equal(t, 0, m.SessionCount())

		m.DeleteSession(sess.id)
	})
}

func TestManagerNewSessionCryptographicQuality(t *testing.T) {
	m, err := NewManager(DefaultOptions())
	require.NoError(t, err)

	sess1, err := m.NewSession(&auth.User{}, nil)
	require.NoError(t, err)

	sess2, err := m.NewSession(&auth.User{}, nil)
	require.NoError(t, err)

	bitsDifference := 0
	for i := 0; i < len(sess1.id) && i < len(sess2.id); i++ {
		b1 := ([]byte(sess1.id))[i]
		b2 := ([]byte(sess2.id))[i]

		diff := bits.OnesCount8(b1 ^ b2)
		bitsDifference += diff
	}

	require.GreaterOrEqual(t, bitsDifference, 90)
}

func TestManagerNewSessionFailureForNoRandomSource(t *testing.T) {
	t.Run("correctly handle error while reading from random source", func(t *testing.T) {
		randSrc := bytes.NewReader(nil)
		opts := DefaultOptions().WithRandSource(randSrc)

		m, err := NewManager(opts)
		require.NoError(t, err)

		_, err = m.NewSession(&auth.User{}, nil)
		require.ErrorIs(t, err, ErrCantCreateSession)
	})

	t.Run("correctly handle not enough data in the random source", func(t *testing.T) {
		randSrc := bytes.NewReader([]byte{0x00})
		opts := DefaultOptions().WithRandSource(randSrc)

		m, err := NewManager(opts)
		require.NoError(t, err)

		_, err = m.NewSession(&auth.User{}, nil)
		require.ErrorIs(t, err, ErrCantCreateSession)
	})
}
