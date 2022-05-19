/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"fmt"
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
func (d *db) ZAdd(req *schema.ZAddRequest) (*schema.TxHeader, error) {
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

	refEntry, err := d.getAtTx(key, req.AtTx, 0, d.st, d.st.NewTxHolder(), 0)
	if err != nil {
		return nil, err
	}
	if refEntry.ReferencedBy != nil {
		return nil, ErrReferencedKeyCannotBeAReference
	}

	tx, err := d.st.NewWriteOnlyTx()
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	e := EncodeZAdd(req.Set, req.Score, key, req.AtTx)

	err = tx.Set(e.Key, e.Metadata, e.Value)
	if err != nil {
		return nil, err
	}

	var hdr *store.TxHeader

	if req.NoWait {
		hdr, err = tx.AsyncCommit()
	} else {
		hdr, err = tx.Commit()
	}
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), nil
}

// ZScan ...
func (d *db) ZScan(req *schema.ZScanRequest) (*schema.ZEntries, error) {
	if req == nil || len(req.Set) == 0 {
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

	d.mutex.RLock()
	defer d.mutex.RUnlock()

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

	snap, err := d.st.SnapshotSince(waitUntilTx)
	if err != nil {
		return nil, err
	}
	defer snap.Close()

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
		if req.Desc {
			var maxScore float64

			if req.MaxScore == nil {
				maxScore = math.MaxFloat64
			} else {
				maxScore = req.MaxScore.Score
			}

			binary.BigEndian.PutUint64(seekKey[len(prefix):], math.Float64bits(maxScore))
		}
	} else {
		seekKey = make([]byte, len(prefix)+scoreLen+keyLenLen+1+len(req.SeekKey)+txIDLen)
		copy(seekKey, prefix)
		binary.BigEndian.PutUint64(seekKey[len(prefix):], math.Float64bits(req.SeekScore))
		binary.BigEndian.PutUint64(seekKey[len(prefix)+scoreLen:], uint64(1+len(req.SeekKey)))
		copy(seekKey[len(prefix)+scoreLen+keyLenLen:], EncodeKey(req.SeekKey))
		binary.BigEndian.PutUint64(seekKey[len(prefix)+scoreLen+keyLenLen+1+len(req.SeekKey):], req.SeekAtTx)
	}

	r, err := snap.NewKeyReader(
		&store.KeyReaderSpec{
			SeekKey:       seekKey,
			Prefix:        prefix,
			InclusiveSeek: req.InclusiveSeek,
			DescOrder:     req.Desc,
			Filters:       []store.FilterFn{store.IgnoreExpired, store.IgnoreDeleted},
		})
	if err != nil {
		return nil, err
	}
	defer r.Close()

	tx := d.st.NewTxHolder()

	entries := &schema.ZEntries{}

	for l := 1; l <= limit; l++ {
		zKey, _, err := r.Read()
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

		e, err := d.getAtTx(key, atTx, 1, snap, tx, 0)
		if err == store.ErrKeyNotFound {
			// ignore deleted ones (referenced key may have been deleted)
			continue
		}
		if err != nil {
			return nil, err
		}

		zentry := &schema.ZEntry{
			Set:   req.Set,
			Key:   key[1:],
			Entry: e,
			Score: score,
			AtTx:  atTx,
		}

		entries.Entries = append(entries.Entries, zentry)

		if l == d.maxResultSize {
			return entries, fmt.Errorf("%w: found at least %d entries (the maximum limit). "+
				"Pagination over large results can be achieved by using the limit, seekKey, seekScore and seekAtTx arguments",
				ErrResultSizeLimitReached, d.maxResultSize)
		}
	}

	return entries, nil
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

	lastTx := d.st.NewTxHolder()

	err = d.st.ReadTx(uint64(txMetatadata.Id), lastTx)
	if err != nil {
		return nil, err
	}

	var prevTx *store.Tx

	if req.ProveSinceTx == 0 {
		prevTx = lastTx
	} else {
		prevTx = d.st.NewTxHolder()

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
		Tx:        schema.TxToProto(lastTx),
		DualProof: schema.DualProofToProto(dualProof),
	}, nil
}
