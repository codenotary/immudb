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

package memory

import (
	"bytes"

	"github.com/codenotary/immudb/v2/embedded/appendable"
)

var _ appendable.Appendable = &memApp{}

func New() appendable.Appendable {
	var buf bytes.Buffer

	return NewWithBuffer(&buf)
}

func NewWithBuffer(buf *bytes.Buffer) appendable.Appendable {
	return &memApp{
		buf: buf,
	}
}

type memApp struct {
	buf *bytes.Buffer
}

func (app *memApp) Metadata() []byte {
	return nil
}

func (app *memApp) ReadAt(dst []byte, off int64) (int, error) {
	buf := app.buf.Bytes()
	n := copy(dst, buf[off:])
	return n, nil
}

func (app *memApp) Offset() int64 {
	return 0
}

func (app *memApp) SetOffset(off int64) error {
	app.buf.Truncate(int(off))
	return nil
}

func (app *memApp) Append(buf []byte) (int64, int, error) {
	off := int64(app.buf.Len())
	n, _ := app.buf.Write(buf)
	return off, n, nil
}

func (app *memApp) DiscardUpto(size int64) error {
	return nil
}

func (app *memApp) Size() (int64, error) {
	return int64(app.buf.Len()), nil
}

func (app *memApp) Flush() error {
	return nil
}

func (app *memApp) Sync() error {
	return nil
}

func (app *memApp) Close() error {
	return nil
}

func (app *memApp) CompressionFormat() int {
	return 0
}

func (app *memApp) CompressionLevel() int {
	return 0
}

func (app *memApp) SwitchToReadOnlyMode() error {
	return nil
}

func (app *memApp) Copy(dstPath string) error {
	return nil
}
