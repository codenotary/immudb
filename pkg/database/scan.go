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

package database

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

// Scan ...
func (d *db) Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	currTxID, _ := d.st.CommittedAlh()

	if req == nil || req.SinceTx > currTxID {
		return nil, store.ErrIllegalArguments
	}

	if req.Limit > uint64(d.maxResultSize) {
		return nil, fmt.Errorf("%w: the specified limit (%d) is larger than the maximum allowed one (%d)",
			ErrResultSizeLimitExceeded, req.Limit, d.maxResultSize)
	}

	limit := int(req.Limit)
	if req.Limit == 0 {
		limit = d.maxResultSize
	}

	seekKey := req.SeekKey
	if len(seekKey) > 0 {
		seekKey = EncodeKey(req.SeekKey)
	}

	endKey := req.EndKey
	if len(endKey) > 0 {
		endKey = EncodeKey(req.EndKey)
	}

	snap, err := d.snapshotSince(ctx, []byte{SetKeyPrefix}, req.SinceTx)
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	r, err := snap.NewKeyReader(
		store.KeyReaderSpec{
			SeekKey:       seekKey,
			EndKey:        endKey,
			Prefix:        EncodeKey(req.Prefix),
			DescOrder:     req.Desc,
			Filters:       []store.FilterFn{store.IgnoreExpired, store.IgnoreDeleted},
			InclusiveSeek: req.InclusiveSeek,
			InclusiveEnd:  req.InclusiveEnd,
			Offset:        req.Offset,
		})
	if err != nil {
		return nil, err
	}
	defer r.Close()

	entries := &schema.Entries{}

	for l := 1; l <= limit; l++ {
		key, valRef, err := r.Read(ctx)
		if errors.Is(err, store.ErrNoMoreEntries) {
			break
		}
		if err != nil {
			return nil, err
		}

		e, err := d.getAtTx(ctx, key, valRef.Tx(), 0, snap, valRef.HC(), true)
		if errors.Is(err, store.ErrKeyNotFound) || errors.Is(err, io.EOF) {
			continue // ignore deleted or truncated ones (referenced key may have been deleted or truncated)
		}
		if err != nil {
			return nil, err
		}

		entries.Entries = append(entries.Entries, e)
	}

	return entries, nil
}
