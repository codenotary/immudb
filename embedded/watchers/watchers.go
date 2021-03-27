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
package watchers

import (
	"errors"
	"sync"
)

var ErrMaxWaitessLimitExceeded = errors.New("max waitess limit exceeded")
var ErrAlreadyClosed = errors.New("already closed")

type WatchersCenter struct {
	wpoints map[uint64]*waitingPoint

	doneUpto uint64 // no-wait on lower or equal values

	maxWaitees int
	waitess    int

	closed bool

	mutex sync.Mutex
}

type waitingPoint struct {
	t     uint64
	ch    chan struct{}
	count int
}

func New(doneUpto uint64, maxWaitees int) *WatchersCenter {
	return &WatchersCenter{
		wpoints:    make(map[uint64]*waitingPoint, 0),
		doneUpto:   doneUpto,
		maxWaitees: maxWaitees,
	}
}

func (w *WatchersCenter) DoneUpto(t uint64) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return ErrAlreadyClosed
	}

	if w.doneUpto >= t {
		return nil
	}

	wp, waiting := w.wpoints[t]
	if waiting {
		close(wp.ch)
		w.waitess -= wp.count
		delete(w.wpoints, t)
	}

	w.doneUpto = t

	return nil
}

func (w *WatchersCenter) WaitFor(t uint64) error {
	w.mutex.Lock()

	if w.closed {
		w.mutex.Unlock()
		return ErrAlreadyClosed
	}

	if w.doneUpto >= t {
		w.mutex.Unlock()
		return nil
	}

	if w.waitess == w.maxWaitees {
		w.mutex.Unlock()
		return ErrMaxWaitessLimitExceeded
	}

	wp, waiting := w.wpoints[t]
	if !waiting {
		wp = &waitingPoint{t: t, ch: make(chan struct{})}
		w.wpoints[t] = wp
	}

	wp.count++
	w.waitess++

	w.mutex.Unlock()

	<-wp.ch

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return ErrAlreadyClosed
	}

	return nil
}

func (w *WatchersCenter) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return ErrAlreadyClosed
	}

	w.closed = true

	for _, wp := range w.wpoints {
		close(wp.ch)
	}

	return nil
}
