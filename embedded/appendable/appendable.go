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

package appendable

import (
	"compress/flate"
	"crypto/sha256"
	"io"
)

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
	DiscardUpto(off int64) error
	Append(bs []byte) (off int64, n int, err error)
	Flush() error
	Sync() error
	SwitchToReadOnlyMode() error
	ReadAt(bs []byte, off int64) (int, error)
	Close() error
	Copy(dstPath string) error
	CompressionFormat() int
	CompressionLevel() int
}

func Checksum(rAt io.ReaderAt, off, n int64) (checksum [sha256.Size]byte, err error) {
	h := sha256.New()
	r := io.NewSectionReader(rAt, off, n)

	c, err := io.Copy(h, r)
	if err != nil {
		return
	}
	if c < n {
		return checksum, io.EOF
	}

	copy(checksum[:], h.Sum(nil))

	return checksum, nil
}
