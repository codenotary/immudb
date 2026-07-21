//go:build wasip1

/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

package main

import "errors"

// ABI error codes. Every ABI call that can fail returns a negative i64 equal to
// one of these; the host then reads the message via imdb_last_error.
const (
	errGeneric    = -1
	errBadRequest = -2
	errBadHandle  = -3
)

var errBadHandleErr = errors.New("invalid store handle")

// lastError holds the message for the most recent failed ABI call (WASM is
// single-threaded, so a single global is safe).
var lastError string

func setError(err error) int64 {
	if err == nil {
		lastError = ""
		return errGeneric
	}
	lastError = err.Error()
	if errors.Is(err, errBadHandleErr) {
		return errBadHandle
	}
	return errGeneric
}
