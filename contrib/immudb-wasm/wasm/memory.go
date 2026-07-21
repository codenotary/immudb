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

import (
	"encoding/hex"
	"unsafe"
)

// hexString hex-encodes bytes (used for root hashes surfaced to the host).
func hexString(b []byte) string {
	return hex.EncodeToString(b)
}

// bytesPtr returns the offset of a byte slice's backing array within the wasm
// linear memory. On wasm32 a pointer is a 32-bit offset the host can index into
// the exported `memory`.
func bytesPtr(b []byte) uint32 {
	if len(b) == 0 {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(&b[0])))
}

// packResult encodes a (pointer, length) pair into a single i64 the ABI returns:
// high 32 bits = pointer, low 32 bits = length. The host splits it and reads the
// result bytes from linear memory, then calls imdb_free(ptr).
func packResult(ptr, length uint32) int64 {
	return int64(ptr)<<32 | int64(length)
}
