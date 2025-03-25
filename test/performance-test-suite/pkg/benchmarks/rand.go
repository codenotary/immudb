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

package benchmarks

import (
	"math/rand"
	"runtime"
)

const hexDigits = "0123456789abcdef"

type randStringGen struct {
	n       int
	done    chan bool
	rndChan chan []byte
}

func (r *randStringGen) randHexString(n int) bool {
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

	select {
	case r.rndChan <- b:
		return true
	case <-r.done:
		return false
	}
}

func NewRandStringGen(size int) *randStringGen {
	ret := &randStringGen{
		rndChan: make(chan []byte, 65536),
		done:    make(chan bool),
	}

	cpu := runtime.NumCPU()
	for j := 0; j < cpu; j++ {
		go func() {
			for ret.randHexString(size) {
			}
		}()
	}

	return ret
}

func (r *randStringGen) GetRnd() []byte {
	return <-r.rndChan
}

func (r *randStringGen) Stop() {
	close(r.done)
}
