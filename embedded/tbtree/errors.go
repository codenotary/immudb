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

package tbtree

import (
	"errors"
	"fmt"

	"github.com/codenotary/immudb/v2/embedded"
)

var (
	ErrCompactionThresholdNotReached = errors.New("tbtree: compaction threshold not yet reached")
	ErrIllegalState                  = errors.New("illegal state")
	ErrIllegalArguments              = fmt.Errorf("tbtree: %w", embedded.ErrIllegalArguments)
	ErrInvalidOptions                = fmt.Errorf("%w: invalid options", ErrIllegalArguments)
	ErrNoMoreEntries                 = errors.New("no more entries")
	ErrOffsetOutOfRange              = errors.New("offset out of range")
	ErrKeyNotFound                   = fmt.Errorf("tbtree: %w", embedded.ErrKeyNotFound)
	ErrorToManyActiveSnapshots       = errors.New("too many active snapshots")
	ErrReadersNotClosed              = errors.New("tbtree: readers not closed")
)
