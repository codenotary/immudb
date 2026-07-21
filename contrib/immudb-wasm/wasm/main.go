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

// Command wasm is the immudb-wasm export layer: it exposes the embedded immudb
// engine (embedded/store + embedded/sql) over a tiny byte-oriented ABI callable
// from a WASI host (Node's node:wasi). It is built as a wasip1 reactor:
//
//	GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o immudb.wasm ./wasm
//
// main is required but unused; the host initializes the reactor and then calls
// the //go:wasmexport functions below. Requests and results are JSON carried in
// linear memory; []byte fields are base64 in JSON, which is binary-safe.
package main

import "encoding/json"

func main() {}

// buffers holds byte slices shared with the host so they survive GC while the
// host reads/writes them, keyed by the linear-memory offset of the backing
// array. WASM is single-threaded, so no locking is needed.
var buffers = map[uint32][]byte{}

//go:wasmexport imdb_alloc
func imdb_alloc(size uint32) uint32 {
	if size == 0 {
		size = 1
	}
	buf := make([]byte, size)
	ptr := bytesPtr(buf)
	buffers[ptr] = buf
	return ptr
}

//go:wasmexport imdb_free
func imdb_free(ptr uint32) {
	delete(buffers, ptr)
}

//go:wasmexport imdb_ping
func imdb_ping(n int32) int32 {
	return n + 1
}

// imdb_last_error copies the most recent error message into the host buffer at
// ptr (capacity cap) and returns the full message length (which may exceed cap).
//
//go:wasmexport imdb_last_error
func imdb_last_error(ptr, capacity uint32) uint32 {
	msg := []byte(lastError)
	dst, ok := buffers[ptr]
	if ok {
		copy(dst[:min(uint32(len(dst)), capacity)], msg)
	}
	return uint32(len(msg))
}

// reqBytes returns the request slice the host wrote into a previously allocated
// buffer.
func reqBytes(ptr, length uint32) []byte {
	b, ok := buffers[ptr]
	if !ok || int(length) > len(b) {
		return nil
	}
	return b[:length]
}

// writeResult allocates a host-readable buffer, copies data in, and returns the
// packed (ptr,len). The host reads it, then calls imdb_free(ptr).
func writeResult(data []byte) int64 {
	ptr := imdb_alloc(uint32(len(data)))
	copy(buffers[ptr], data)
	return packResult(ptr, uint32(len(data)))
}

func jsonResult(v any) int64 {
	data, err := json.Marshal(v)
	if err != nil {
		return setError(err)
	}
	return writeResult(data)
}

func decode(ptr, length uint32, v any) bool {
	return json.Unmarshal(reqBytes(ptr, length), v) == nil
}

// --- ABI: lifecycle -------------------------------------------------------

type openReq struct {
	Path string `json:"path"`
}

// imdb_open opens (creating if needed) a store and returns a positive handle, or
// a negative error code.
//
//go:wasmexport imdb_open
func imdb_open(reqPtr, reqLen uint32) int64 {
	var req openReq
	if !decode(reqPtr, reqLen, &req) || req.Path == "" {
		return errBadRequest
	}
	h, err := openStore(req.Path)
	if err != nil {
		return setError(err)
	}
	return int64(h)
}

//go:wasmexport imdb_close
func imdb_close(handle int32) int32 {
	if err := closeStore(handle); err != nil {
		setError(err)
		return errBadHandle
	}
	return 0
}

// --- ABI: key-value -------------------------------------------------------

type setReq struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

//go:wasmexport imdb_set
func imdb_set(handle int32, reqPtr, reqLen uint32) int64 {
	e, err := lookup(handle)
	if err != nil {
		return setError(err)
	}
	var req setReq
	if !decode(reqPtr, reqLen, &req) {
		return errBadRequest
	}
	txID, err := e.set(req.Key, req.Value)
	if err != nil {
		return setError(err)
	}
	return jsonResult(map[string]any{"tx": txID})
}

type keyReq struct {
	Key []byte `json:"key"`
}

//go:wasmexport imdb_get
func imdb_get(handle int32, reqPtr, reqLen uint32) int64 {
	e, err := lookup(handle)
	if err != nil {
		return setError(err)
	}
	var req keyReq
	if !decode(reqPtr, reqLen, &req) {
		return errBadRequest
	}
	value, txID, found, err := e.get(req.Key)
	if err != nil {
		return setError(err)
	}
	return jsonResult(map[string]any{"found": found, "value": value, "tx": txID})
}

type scanReq struct {
	Prefix []byte `json:"prefix"`
	Limit  int    `json:"limit"`
}

//go:wasmexport imdb_scan
func imdb_scan(handle int32, reqPtr, reqLen uint32) int64 {
	e, err := lookup(handle)
	if err != nil {
		return setError(err)
	}
	var req scanReq
	if !decode(reqPtr, reqLen, &req) {
		return errBadRequest
	}
	entries, err := e.scan(req.Prefix, req.Limit)
	if err != nil {
		return setError(err)
	}
	return jsonResult(map[string]any{"entries": entries})
}

//go:wasmexport imdb_verified_get
func imdb_verified_get(handle int32, reqPtr, reqLen uint32) int64 {
	e, err := lookup(handle)
	if err != nil {
		return setError(err)
	}
	var req keyReq
	if !decode(reqPtr, reqLen, &req) {
		return errBadRequest
	}
	res, err := e.verifiedGet(req.Key)
	if err != nil {
		return setError(err)
	}
	return jsonResult(res)
}

// --- ABI: SQL -------------------------------------------------------------

type sqlReq struct {
	SQL string `json:"sql"`
}

//go:wasmexport imdb_sql_exec
func imdb_sql_exec(handle int32, reqPtr, reqLen uint32) int64 {
	e, err := lookup(handle)
	if err != nil {
		return setError(err)
	}
	var req sqlReq
	if !decode(reqPtr, reqLen, &req) {
		return errBadRequest
	}
	if err := e.sqlExec(req.SQL); err != nil {
		return setError(err)
	}
	return jsonResult(map[string]any{"ok": true})
}

//go:wasmexport imdb_sql_query
func imdb_sql_query(handle int32, reqPtr, reqLen uint32) int64 {
	e, err := lookup(handle)
	if err != nil {
		return setError(err)
	}
	var req sqlReq
	if !decode(reqPtr, reqLen, &req) {
		return errBadRequest
	}
	res, err := e.sqlQuery(req.SQL)
	if err != nil {
		return setError(err)
	}
	return jsonResult(res)
}
