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
package writetxs

import (
	"math/rand"
	"runtime"
	"sync/atomic"
)

const hexDigits = "0123456789abcdef"

type randStringGen struct {
	n       int
	done    int32
	rndChan chan []byte
}

func (r *randStringGen) randHexString(n int) {
	b := make([]byte, n)
	for i, offset := n/4, 0; i > 0; i-- {
		// A Int63() generates 63 random bits
		cache := rand.Int63()
		for j := 0; j < 4; j++ {
			idx := int(cache & 15)
			b[offset] = hexDigits[idx]
			cache >>= 4
			offset++
		}
	}
	r.rndChan <- b
}

func NewRandStringGen(size int) *randStringGen {
	ret := &randStringGen{
		rndChan: make(chan []byte, 65536),
	}

	cpu := runtime.NumCPU()
	for j := 0; j < cpu; j++ {
		go func() {
			for atomic.LoadInt32(&ret.done) == 0 {
				ret.randHexString(size)
			}
		}()
	}

	return ret
}

func (r *randStringGen) getRnd() []byte {
	return <-r.rndChan
}

func (r *randStringGen) stop() {
	atomic.StoreInt32(&r.done, 1)
}
