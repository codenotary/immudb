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
package appendable

import "compress/flate"

const DefaultBlockSize = 4096
const DefaultCompressionFormat = NoCompression
const DefaultCompressionLevel = BestSpeed

const (
	NoCompression = iota
	FlateCompression
	GZipCompression
	LZWCompression
	ZLibCompression
)

const (
	BestSpeed          = flate.BestSpeed
	BestCompression    = flate.BestCompression
	DefaultCompression = flate.DefaultCompression
	HuffmanOnly        = flate.HuffmanOnly
)

type Appendable interface {
	Metadata() []byte
	Size() (int64, error)
	Offset() int64
	SetOffset(off int64) error
	Append(bs []byte) (off int64, n int, err error)
	Flush() error
	Sync() error
	ReadAt(bs []byte, off int64) (int, error)
	Close() error
	Copy(dstPath string) error
	CompressionFormat() int
	CompressionLevel() int
}

func PaddingLen(sz, blockSize int) int {
	if sz < blockSize {
		return blockSize - sz
	} else {
		return sz % blockSize
	}
}
