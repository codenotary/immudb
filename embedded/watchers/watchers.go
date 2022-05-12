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
package watchers

import (
	"errors"
	"sync"
)

var ErrMaxWaitessLimitExceeded = errors.New("max waiting limit exceeded")
var ErrAlreadyClosed = errors.New("already closed")
var ErrCancellationRequested = errors.New("cancellation requested")

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

func (w *WatchersHub) WaitFor(t uint64, cancellation <-chan struct{}) error {
	w.mutex.Lock()

	if w.closed {
		w.mutex.Unlock()
		return ErrAlreadyClosed
	}

	if w.doneUpto >= t {
		w.mutex.Unlock()
		return nil
	}

	if w.waiting == w.maxWaiting {
		w.mutex.Unlock()
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

	if cancellation == nil {
		<-wp.ch
	} else {
		select {
		case <-wp.ch:
			{
				break
			}
		case <-cancellation:
			{
				w.mutex.Lock()
				defer w.mutex.Unlock()

				if w.closed {
					return ErrAlreadyClosed
				}

				if wp.count == 1 {
					close(wp.ch)
					delete(w.wpoints, t)
				}

				if wp.count > 0 {
					w.waiting--
				}

				return ErrCancellationRequested
			}
		}
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return ErrAlreadyClosed
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
