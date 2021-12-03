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

import "time"

type Options struct {
	SessionGuardCheckInterval time.Duration
	// MaxSessionIdleTime is a duration for the amount of time after which an idle session would be closed by the server
	MaxSessionIdleTime time.Duration
	// MaxSessionAgeTime is a duration for the maximum amount of time a session may exist before it will be closed by the server
	MaxSessionAgeTime time.Duration
	// Timeout the server waits for a duration of Timeout and if no activity is seen even after that the session is closed
	Timeout time.Duration
}

func DefaultOptions() *Options {
	return &Options{
		SessionGuardCheckInterval: time.Second * 1,
		MaxSessionIdleTime:        time.Second * 5,
		MaxSessionAgeTime:         time.Second * 300,
		Timeout:                   time.Second * 7,
	}
}
