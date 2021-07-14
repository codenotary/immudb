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
	"encoding/binary"
	"math"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

const setLenLen = 8
const scoreLen = 8
const keyLenLen = 8
const txIDLen = 8

// ZAdd adds a score for an existing key in a sorted set
// As a parameter of ZAddOptions is possible to provide the associated index of the provided key. In this way, when resolving reference, the specified version of the key will be returned.
// If the index is not provided the resolution will use only the key and last version of the item will be returned
// If ZAddOptions.index is provided key is optional
func (d *db) ZAdd(req *schema.ZAddRequest) (*schema.TxMetadata, error) {
	if req == nil || len(req.Set) == 0 || len(req.Key) == 0 {
		return nil, store.ErrIllegalArguments
	}

	if (req.AtTx == 0 && req.BoundRef) || (req.AtTx > 0 && !req.BoundRef) {
		return nil, store.ErrIllegalArguments
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	lastTxID, _ := d.st.Alh()
	err := d.st.WaitForIndexingUpto(lastTxID, nil)
	if err != nil {
		return nil, err
	}

	// check referenced key exists and it's not a reference
	key := EncodeKey(req.Key)

	refEntry, err := d.getAt(key, req.AtTx, 0, d.st, d.tx1)
	if err != nil {
		return nil, err
	}
	if refEntry.ReferencedBy != nil {
		return nil, ErrReferencedKeyCannotBeAReference
	}

	meta, err := d.st.Commit([]*store.KV{EncodeZAdd(req.Set, req.Score, key, req.AtTx)}, !req.NoWait)

	return schema.TxMetatadaTo(meta), err
}

// ZScan ...
func (d *db) ZScan(req *schema.ZScanRequest) (*schema.ZEntries, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if req == nil || len(req.Set) == 0 {
		return nil, store.ErrIllegalArguments
	}

	if req.Limit > MaxKeyScanLimit {
		return nil, ErrMaxKeyScanLimitExceeded
	}

	limit := req.Limit

	if req.Limit == 0 {
		limit = MaxKeyScanLimit
	}

	prefix := make([]byte, 1+setLenLen+len(req.Set))
	prefix[0] = SortedSetKeyPrefix
	binary.BigEndian.PutUint64(prefix[1:], uint64(len(req.Set)))
	copy(prefix[1+setLenLen:], req.Set)

	var seekKey []byte

	if len(req.SeekKey) == 0 {
		seekKey = make([]byte, len(prefix)+scoreLen)
		copy(seekKey, prefix)
		// here we compose the offset if Min score filter is provided only if is not reversed order
		if req.MinScore != nil && !req.Desc {
			binary.BigEndian.PutUint64(seekKey[len(prefix):], math.Float64bits(req.MinScore.Score))
		}
		// here we compose the offset if Max score filter is provided only if is reversed order
		if req.MaxScore != nil && req.Desc {
			binary.BigEndian.PutUint64(seekKey[len(prefix):], math.Float64bits(req.MaxScore.Score))
		}
	} else {
		seekKey = make([]byte, len(prefix)+scoreLen+keyLenLen+1+len(req.SeekKey)+txIDLen)
		copy(seekKey, prefix)
		binary.BigEndian.PutUint64(seekKey[len(prefix):], math.Float64bits(req.SeekScore))
		binary.BigEndian.PutUint64(seekKey[len(prefix)+scoreLen:], uint64(1+len(req.SeekKey)))
		copy(seekKey[len(prefix)+scoreLen+keyLenLen:], EncodeKey(req.SeekKey))
		binary.BigEndian.PutUint64(seekKey[len(prefix)+scoreLen+keyLenLen+1+len(req.SeekKey):], req.SeekAtTx)
	}

	waitUntilTx := req.SinceTx

	if waitUntilTx == 0 {
		waitUntilTx, _ = d.st.Alh()
	}

	if !req.NoWait {
		err := d.st.WaitForIndexingUpto(waitUntilTx, nil)
		if err != nil {
			return nil, err
		}
	}

	snap, err := d.st.SnapshotSince(waitUntilTx)
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	r, err := snap.NewKeyReader(
		&store.KeyReaderSpec{
			SeekKey:       seekKey,
			Prefix:        prefix,
			InclusiveSeek: req.InclusiveSeek,
			DescOrder:     req.Desc,
		})
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var entries []*schema.ZEntry
	i := uint64(0)

	for {
		zKey, _, _, _, err := r.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		// zKey = [1+setLenLen+len(req.Set)+scoreLen+keyLenLen+1+len(req.Key)+txIDLen]
		scoreOff := 1 + setLenLen + len(req.Set)
		scoreB := binary.BigEndian.Uint64(zKey[scoreOff:])
		score := math.Float64frombits(scoreB)

		// Guard to ensure that score match the filter range if filter is provided
		if req.MinScore != nil && score < req.MinScore.Score {
			continue
		}
		if req.MaxScore != nil && score > req.MaxScore.Score {
			continue
		}

		keyOff := scoreOff + scoreLen + keyLenLen
		key := make([]byte, len(zKey)-keyOff-txIDLen)
		copy(key, zKey[keyOff:])

		atTx := binary.BigEndian.Uint64(zKey[keyOff+len(key):])

		e, err := d.getAt(key, atTx, 0, snap, d.tx1)

		zentry := &schema.ZEntry{
			Set:   req.Set,
			Key:   key[1:],
			Entry: e,
			Score: score,
			AtTx:  atTx,
		}

		entries = append(entries, zentry)
		if i++; i == limit {
			break
		}
	}

	list := &schema.ZEntries{
		Entries: entries,
	}

	return list, nil
}

//VerifiableZAdd ...
func (d *db) VerifiableZAdd(req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error) {
	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	lastTxID, _ := d.st.Alh()
	if lastTxID < req.ProveSinceTx {
		return nil, store.ErrIllegalArguments
	}

	txMetatadata, err := d.ZAdd(req.ZAddRequest)
	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	lastTx := d.tx1

	err = d.st.ReadTx(uint64(txMetatadata.Id), lastTx)
	if err != nil {
		return nil, err
	}

	var prevTx *store.Tx

	if req.ProveSinceTx == 0 {
		prevTx = lastTx
	} else {
		prevTx = d.tx2

		err = d.st.ReadTx(req.ProveSinceTx, prevTx)
		if err != nil {
			return nil, err
		}
	}

	dualProof, err := d.st.DualProof(prevTx, lastTx)
	if err != nil {
		return nil, err
	}

	return &schema.VerifiableTx{
		Tx:        schema.TxTo(lastTx),
		DualProof: schema.DualProofTo(dualProof),
	}, nil
}
