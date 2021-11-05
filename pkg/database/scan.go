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
package database

import (
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

//Scan ...
func (d *db) Scan(req *schema.ScanRequest) (*schema.Entries, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	if req.Limit > MaxKeyScanLimit {
		return nil, ErrMaxKeyScanLimitExceeded
	}

	currTxID, _ := d.st.Alh()

	if req.SinceTx > currTxID {
		return nil, ErrIllegalArguments
	}

	waitUntilTx := req.SinceTx
	if waitUntilTx == 0 {
		waitUntilTx = currTxID
	}

	if !req.NoWait {
		err := d.st.WaitForIndexingUpto(waitUntilTx, nil)
		if err != nil {
			return nil, err
		}
	}

	limit := req.Limit

	if req.Limit == 0 {
		limit = MaxKeyScanLimit
	}

	var entries []*schema.Entry
	i := uint64(0)

	snap, err := d.st.SnapshotSince(waitUntilTx)
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	seekKey := req.SeekKey

	if len(seekKey) > 0 {
		seekKey = EncodeKey(req.SeekKey)
	}

	r, err := snap.NewKeyReader(
		&store.KeyReaderSpec{
			SeekKey:   seekKey,
			Prefix:    EncodeKey(req.Prefix),
			DescOrder: req.Desc,
			Filter:    store.IgnoreDeleted,
		})
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for {
		key, valRef, err := r.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		e, err := d.getAt(key, valRef.Tx(), 0, snap, d.tx1)
		if err == store.ErrKeyNotFound {
			// ignore deleted ones (referenced key may have been deleted)
			continue
		}
		if err != nil {
			return nil, err
		}

		entries = append(entries, e)
		if i++; i == limit {
			break
		}
	}

	return &schema.Entries{
		Entries: entries,
	}, nil
}
