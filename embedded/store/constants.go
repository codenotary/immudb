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

package store

import "crypto/sha256"

const (
	MaxKeyLen     = 1024 // assumed to be not lower than hash size
	MaxParallelIO = 127

	cLogEntrySizeV1 = offsetSize + lszSize               // tx offset + hdr size
	cLogEntrySizeV2 = offsetSize + lszSize + sha256.Size // tx offset + hdr size + alh

	txIDSize   = 8
	tsSize     = 8
	lszSize    = 4
	sszSize    = 2
	offsetSize = 8

	// Version 2 includes `metaEmbeddedValues` and `metaPreallocFiles` into clog metadata
	Version = 2

	MaxTxHeaderVersion = 1

	metaVersion        = "VERSION"
	metaMaxTxEntries   = "MAX_TX_ENTRIES"
	metaMaxKeyLen      = "MAX_KEY_LEN"
	metaMaxValueLen    = "MAX_VALUE_LEN"
	metaFileSize       = "FILE_SIZE"
	metaEmbeddedValues = "EMBEDDED_VALUES"
	metaPreallocFiles  = "PREALLOC_FILES"

	indexDirname = "index"
	ahtDirname   = "aht"
)
