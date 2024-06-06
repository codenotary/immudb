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

package tbtree

type HistoryReaderSpec struct {
	Key       []byte
	Offset    uint64
	DescOrder bool
	ReadLimit int
}

type HistoryReader struct {
	id       int
	snapshot *Snapshot
	closed   bool

	key       []byte
	offset    uint64
	descOrder bool
	readLimit int
}

func newHistoryReader(id int, snap *Snapshot, spec *HistoryReaderSpec) (*HistoryReader, error) {
	if spec == nil {
		return nil, ErrIllegalArguments
	}

	//TODO (jeroiraz): locate leafnode at which `key`is stored so to avoid searching on the tree on each Read call

	return &HistoryReader{
		id:       id,
		snapshot: snap,
		closed:   false,

		key:       spec.Key,
		offset:    spec.Offset,
		descOrder: spec.DescOrder,
		readLimit: spec.ReadLimit,
	}, nil
}

func (r *HistoryReader) Read() ([]TimedValue, error) {
	if r.closed {
		return nil, ErrAlreadyClosed
	}

	timedValues, _, err := r.snapshot.History(r.key, r.offset, r.descOrder, r.readLimit)
	if err != nil {
		return nil, err
	}

	r.offset += uint64(len(timedValues))

	return timedValues, nil
}

func (r *HistoryReader) Close() error {
	if r.closed {
		return ErrAlreadyClosed
	}

	r.snapshot.closedReader(r.id)
	r.closed = true

	return nil
}
