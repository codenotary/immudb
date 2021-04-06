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
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/pkg/api/schema"
)

//Scan ...
func (d *db) Scan(req *schema.ScanRequest) (*schema.Entries, error) {
	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	if req.Limit > MaxKeyScanLimit {
		return nil, ErrMaxKeyScanLimitExceeded
	}

	if !req.NoWait {
		err := d.st.WaitForIndexingUpto(req.SinceTx)
		if err != nil {
			return nil, err
		}
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	limit := req.Limit

	if req.Limit == 0 {
		limit = MaxKeyScanLimit
	}

	var entries []*schema.Entry
	i := uint64(0)

	snap, err := d.st.SnapshotSince(req.SinceTx)
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	seekKey := req.SeekKey

	if len(seekKey) > 0 {
		seekKey = EncodeKey(req.SeekKey)
	}

	r, err := d.st.NewKeyReader(
		snap,
		&tbtree.ReaderSpec{
			SeekKey:   seekKey,
			Prefix:    EncodeKey(req.Prefix),
			DescOrder: req.Desc,
		})
	if err == store.ErrNoMoreEntries {
		return &schema.Entries{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for {
		key, _, tx, _, err := r.Read()
		if err == tbtree.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		e, err := d.getAt(key, tx, 0, snap, d.tx1)
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
