/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
	"crypto/rand"
	"fmt"
	"io"
	"time"
)

type Options struct {
	SessionGuardCheckInterval time.Duration
	// MaxSessionInactivityTime is a duration for the amount of time after which an idle session would be closed by the server
	MaxSessionInactivityTime time.Duration
	// MaxSessionAgeTime is a duration for the maximum amount of time a session may exist before it will be closed by the server
	MaxSessionAgeTime time.Duration
	// Timeout the server waits for a duration of Timeout and if no activity is seen even after that the session is closed
	Timeout time.Duration
	// Max number of simultaneous sessions
	MaxSessions int
	// Random number generator
	RandSource io.Reader
}

func DefaultOptions() *Options {
	return &Options{
		SessionGuardCheckInterval: time.Minute * 1,
		MaxSessionInactivityTime:  time.Minute * 3,
		MaxSessionAgeTime:         infinity,
		Timeout:                   time.Minute * 2,
		MaxSessions:               100,
		RandSource:                rand.Reader,
	}
}

func (o *Options) WithSessionGuardCheckInterval(interval time.Duration) *Options {
	o.SessionGuardCheckInterval = interval
	return o
}
func (o *Options) WithMaxSessionInactivityTime(maxInactivityTime time.Duration) *Options {
	o.MaxSessionInactivityTime = maxInactivityTime
	return o
}

func (o *Options) WithMaxSessionAgeTime(maxAgeTime time.Duration) *Options {
	o.MaxSessionAgeTime = maxAgeTime
	return o
}

func (o *Options) WithTimeout(timeout time.Duration) *Options {
	o.Timeout = timeout
	return o
}

func (o *Options) WithMaxSessions(maxSessions int) *Options {
	o.MaxSessions = maxSessions
	return o
}

func (o *Options) WithRandSource(src io.Reader) *Options {
	o.RandSource = src
	return o
}

func (o *Options) Validate() error {
	if o.MaxSessionAgeTime < 0 {
		return fmt.Errorf("%w: invalid MaxSessionAgeTime", ErrInvalidOptionsProvided)
	}
	if o.MaxSessionInactivityTime < 0 {
		return fmt.Errorf("%w: invalid MaxSessionInactivityTime", ErrInvalidOptionsProvided)
	}
	if o.Timeout < 0 {
		return fmt.Errorf("%w: invalid Timeout", ErrInvalidOptionsProvided)
	}
	if o.SessionGuardCheckInterval <= 0 {
		return fmt.Errorf("%w: invalid SessionGuardCheckInterval", ErrInvalidOptionsProvided)
	}
	if o.MaxSessions <= 0 {
		return fmt.Errorf("%w: invalid MaxSessions", ErrInvalidOptionsProvided)
	}
	if o.RandSource == nil {
		return fmt.Errorf("%w: invalid RandSource", ErrInvalidOptionsProvided)
	}
	return nil
}

func (o *Options) Normalize() *Options {
	if o.MaxSessionAgeTime == 0 {
		o.MaxSessionAgeTime = infinity
	}
	if o.MaxSessionInactivityTime == 0 {
		o.MaxSessionInactivityTime = infinity
	}
	if o.Timeout == 0 {
		o.Timeout = infinity
	}
	return o
}
