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

package appendable

import (
	"crypto/sha256"
	"hash"
)

type ChecksumAppendable struct {
	Appendable

	h hash.Hash
}

func WithChecksum(app Appendable) *ChecksumAppendable {
	return &ChecksumAppendable{
		Appendable: app,
		h:          sha256.New(),
	}
}

func (app *ChecksumAppendable) Append(bs []byte) (off int64, n int, err error) {
	off, n, err = app.Appendable.Append(bs)
	if err == nil {
		_, _ = app.h.Write(bs)
	}
	return off, n, err
}

func (app *ChecksumAppendable) Sum(b []byte) (checksum [sha256.Size]byte) {
	_, _ = app.h.Write(b)

	copy(checksum[:], app.h.Sum(nil))
	return checksum
}
