/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

import (
	"math/rand"
	"runtime"
	"time"
)

const hexDigits = "0123456789abcdef"

func randHexString(n int) {
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
	rndChan <- b
}

var rndChan chan []byte

func startRnd(size int) {
	rand.Seed(time.Now().UnixNano())
	rndChan = make(chan []byte, 65536)
	cpu := runtime.NumCPU()
	for j := 0; j < cpu; j++ {
		go func() {
			for {
				randHexString(size)
			}
		}()
	}
}

func getRnd() string {
	ret := string(<-rndChan)
	return ret
}

func getPayload(size int) []byte {
	b := make([]byte, 0, size)
	for len(b) < size {
		b = append(b, <-rndChan...)
	}
	return b
}
