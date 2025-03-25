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

package watchers

import (
	"context"
	"errors"
	"sync"
)

var ErrMaxWaitessLimitExceeded = errors.New("watchers: max waiting limit exceeded")
var ErrAlreadyClosed = errors.New("watchers: already closed")
var ErrIllegalState = errors.New("watchers: illegal state")

type WatchersHub struct {
	wpoints map[uint64]*waitingPoint

	doneUpto uint64 // no-wait on lower or equal values

	maxWaiting int
	waiting    int

	closed bool

	mutex sync.Mutex
}

type waitingPoint struct {
	t     uint64
	ch    chan struct{}
	count int
}

func New(doneUpto uint64, maxWaiting int) *WatchersHub {
	return &WatchersHub{
		wpoints:    make(map[uint64]*waitingPoint, 0),
		doneUpto:   doneUpto,
		maxWaiting: maxWaiting,
	}
}

func (w *WatchersHub) Status() (doneUpto uint64, waiting int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return 0, 0, ErrAlreadyClosed
	}

	return w.doneUpto, w.waiting, nil
}

func (w *WatchersHub) RecedeTo(t uint64) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return ErrAlreadyClosed
	}

	if w.doneUpto < t {
		return ErrIllegalState
	}

	w.doneUpto = t

	return nil
}

func (w *WatchersHub) DoneUpto(t uint64) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return ErrAlreadyClosed
	}

	if w.doneUpto >= t {
		return nil
	}

	for i := w.doneUpto + 1; i <= t; i++ {
		if w.waiting == 0 {
			break
		}

		wp, waiting := w.wpoints[i]
		if waiting {
			close(wp.ch)
			w.waiting -= wp.count
			wp.count = 0
			delete(w.wpoints, i)
		}
	}

	w.doneUpto = t

	return nil
}

func (w *WatchersHub) WaitFor(ctx context.Context, t uint64) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return ErrAlreadyClosed
	}

	if w.doneUpto >= t {
		return nil
	}

	if w.waiting == w.maxWaiting {
		return ErrMaxWaitessLimitExceeded
	}

	wp, waiting := w.wpoints[t]
	if !waiting {
		wp = &waitingPoint{t: t, ch: make(chan struct{})}
		w.wpoints[t] = wp
	}

	wp.count++
	w.waiting++

	w.mutex.Unlock()

	cancelled := false

	select {
	case <-wp.ch:
		break
	case <-ctx.Done():
		cancelled = true
	}

	w.mutex.Lock()

	if w.closed {
		return ErrAlreadyClosed
	}

	if cancelled {

		// `wp.count` will be zeroed if the `t` point was already processed in
		// `DoneUpTo` call (a situation when both cancellation and call to `DoneUpTo`
		// happen simultaneously), otherwise its necessary to cleanup after we stopped waiting.
		if wp.count > 0 {
			w.waiting--
			wp.count--

			if wp.count == 0 {
				// This was the last `WaitFor`` caller waiting for the point `t``,
				// cleanup the `w.wpoints` array to avoid holding idle entries there.
				close(wp.ch)
				delete(w.wpoints, t)
			}
		}

		return ctx.Err()
	}

	return nil
}

func (w *WatchersHub) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return ErrAlreadyClosed
	}

	w.closed = true

	for _, wp := range w.wpoints {
		close(wp.ch)
		w.waiting -= wp.count
		wp.count = 0
	}

	return nil
}
