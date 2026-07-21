//go:build wasip1
// +build wasip1

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

package fileutils

import "os"

// WASI preview 1 has no primitive to sync a directory's entries, so syncDir is a
// no-op here. This is a durability trade-off documented for the WASM build: a
// crash immediately after a file is created (but before the parent directory
// entry is durably flushed) could lose that file on some host filesystems.
func syncDir(path string) error {
	return nil
}

// fdatasync falls back to a full file sync; os.File.Sync maps to the WASI
// fd_sync call, which flushes file data and metadata.
func fdatasync(f *os.File) error {
	return f.Sync()
}
