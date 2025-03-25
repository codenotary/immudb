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

package embedded

import "errors"

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("already closed")
var ErrKeyNotFound = errors.New("key not found")
var ErrOffsetOutOfRange = errors.New("offset out of range")
var ErrIllegalState = errors.New("illegal state")
var ErrNoMoreEntries = errors.New("no more entries")
var ErrReadersNotClosed = errors.New("readers not closed")
