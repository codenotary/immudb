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
	"crypto/sha256"
	"strconv"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestStoreReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	req := &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}}
	meta, err := db.Set(req)

	item, err := db.Get(&schema.KeyRequest{Key: []byte(`firstKey`), SinceTx: meta.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`firstKey`), item.Key)
	require.Equal(t, []byte(`firstValue`), item.Value)

	refOpts := &schema.ReferenceRequest{
		Key:           []byte(`myTag`),
		ReferencedKey: []byte(`secondKey`),
	}
	meta, err = db.SetReference(refOpts)
	require.Equal(t, store.ErrKeyNotFound, err)

	refOpts = &schema.ReferenceRequest{
		Key:           []byte(`firstKeyR`),
		ReferencedKey: []byte(`firstKey`),
		AtTx:          0,
		BoundRef:      true,
	}
	_, err = db.SetReference(refOpts)
	require.Equal(t, store.ErrIllegalArguments, err)

	refOpts = &schema.ReferenceRequest{
		Key:           []byte(`firstKey`),
		ReferencedKey: []byte(`firstKey`),
	}
	meta, err = db.SetReference(refOpts)
	require.Equal(t, ErrFinalKeyCannotBeConvertedIntoReference, err)

	refOpts = &schema.ReferenceRequest{
		Key:           []byte(`myTag`),
		ReferencedKey: []byte(`firstKey`),
	}
	meta, err = db.SetReference(refOpts)
	require.NoError(t, err)
	require.Equal(t, uint64(3), meta.Id)

	keyReq := &schema.KeyRequest{Key: []byte(`myTag`), SinceTx: meta.Id}

	firstItemRet, err := db.Get(keyReq)
	require.NoError(t, err)
	require.Equal(t, []byte(`firstValue`), firstItemRet.Value, "Should have referenced item value")

	vitem, err := db.VerifiableGet(&schema.VerifiableGetRequest{
		KeyRequest:   keyReq,
		ProveSinceTx: 1,
	})
	require.NoError(t, err)
	require.Equal(t, []byte(`firstKey`), vitem.Entry.Key)
	require.Equal(t, []byte(`firstValue`), vitem.Entry.Value)

	inclusionProof := schema.InclusionProofFrom(vitem.InclusionProof)

	var eh [sha256.Size]byte
	copy(eh[:], vitem.VerifiableTx.Tx.Metadata.EH)

	verifies := store.VerifyInclusion(
		inclusionProof,
		EncodeReference([]byte(`myTag`), []byte(`firstKey`), 0),
		eh,
	)
	require.True(t, verifies)
}

func TestStore_GetReferenceWithIndexResolution(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	set, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`value1`)}}})
	require.NoError(t, err)

	_, err = db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`value2`)}}})
	require.NoError(t, err)

	ref, err := db.SetReference(&schema.ReferenceRequest{Key: []byte(`myTag1`), ReferencedKey: []byte(`aaa`), AtTx: set.Id, BoundRef: true})
	require.NoError(t, err)

	tag3, err := db.Get(&schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: ref.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag3.Key)
	require.Equal(t, []byte(`value1`), tag3.Value)
}

func TestStoreInvalidReferenceToReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	req := &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}}
	meta, err := db.Set(req)

	ref1, err := db.SetReference(&schema.ReferenceRequest{Key: []byte(`myTag1`), ReferencedKey: []byte(`firstKey`), AtTx: meta.Id, BoundRef: true})
	require.NoError(t, err)

	_, err = db.Get(&schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: ref1.Id})
	require.NoError(t, err)

	_, err = db.SetReference(&schema.ReferenceRequest{Key: []byte(`myTag2`), ReferencedKey: []byte(`myTag1`)})
	require.Equal(t, ErrReferencedKeyCannotBeAReference, err)
}

func TestStoreReferenceAsyncCommit(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	firstIndex, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}})
	require.NoError(t, err)

	secondIndex, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`secondKey`), Value: []byte(`secondValue`)}}})
	require.NoError(t, err)

	for n := uint64(0); n <= 64; n++ {
		tag := []byte(strconv.FormatUint(n, 10))
		var itemKey []byte
		var atTx uint64

		if n%2 == 0 {
			itemKey = []byte(`firstKey`)
			atTx = firstIndex.Id
		} else {
			itemKey = []byte(`secondKey`)
			atTx = secondIndex.Id
		}

		refOpts := &schema.ReferenceRequest{
			Key:           tag,
			ReferencedKey: itemKey,
			AtTx:          atTx,
			BoundRef:      true,
		}

		ref, err := db.SetReference(refOpts)
		require.NoError(t, err, "n=%d", n)
		require.Equal(t, n+2+2, ref.Id, "n=%d", n)
	}

	for n := uint64(0); n <= 64; n++ {
		tag := []byte(strconv.FormatUint(n, 10))
		var itemKey []byte
		var itemVal []byte
		var index uint64
		if n%2 == 0 {
			itemKey = []byte(`firstKey`)
			itemVal = []byte(`firstValue`)
			index = firstIndex.Id
		} else {
			itemKey = []byte(`secondKey`)
			itemVal = []byte(`secondValue`)
			index = secondIndex.Id
		}

		item, err := db.Get(&schema.KeyRequest{Key: tag, SinceTx: 67})
		require.NoError(t, err, "n=%d", n)
		require.Equal(t, index, item.Tx, "n=%d", n)
		require.Equal(t, itemVal, item.Value, "n=%d", n)
		require.Equal(t, itemKey, item.Key, "n=%d", n)
	}
}

func TestStoreMultipleReferenceOnSameKey(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx0, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}})
	require.NoError(t, err)

	idx1, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`secondKey`), Value: []byte(`secondValue`)}}})
	require.NoError(t, err)

	refOpts1 := &schema.ReferenceRequest{
		Key:           []byte(`myTag1`),
		ReferencedKey: []byte(`firstKey`),
		AtTx:          idx0.Id,
		BoundRef:      true,
	}

	reference1, err := db.SetReference(refOpts1)
	require.NoError(t, err)
	require.Exactly(t, uint64(4), reference1.Id)
	require.NotEmptyf(t, reference1, "Should not be empty")

	refOpts2 := &schema.ReferenceRequest{
		Key:           []byte(`myTag2`),
		ReferencedKey: []byte(`firstKey`),
		AtTx:          idx0.Id,
		BoundRef:      true,
	}
	reference2, err := db.SetReference(refOpts2)
	require.NoError(t, err)
	require.Exactly(t, uint64(5), reference2.Id)
	require.NotEmptyf(t, reference2, "Should not be empty")

	refOpts3 := &schema.ReferenceRequest{
		Key:           []byte(`myTag3`),
		ReferencedKey: []byte(`secondKey`),
		AtTx:          idx1.Id,
		BoundRef:      true,
	}
	reference3, err := db.SetReference(refOpts3)
	require.NoError(t, err)
	require.Exactly(t, uint64(6), reference3.Id)
	require.NotEmptyf(t, reference3, "Should not be empty")

	firstTagRet, err := db.Get(&schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: reference3.Id})
	require.NoError(t, err)
	require.NotEmptyf(t, firstTagRet, "Should not be empty")
	require.Equal(t, []byte(`firstValue`), firstTagRet.Value, "Should have referenced item value")

	secondTagRet, err := db.Get(&schema.KeyRequest{Key: []byte(`myTag2`), SinceTx: reference3.Id})
	require.NoError(t, err)
	require.NotEmptyf(t, secondTagRet, "Should not be empty")
	require.Equal(t, []byte(`firstValue`), secondTagRet.Value, "Should have referenced item value")

	thirdItemRet, err := db.Get(&schema.KeyRequest{Key: []byte(`myTag3`), SinceTx: reference3.Id})
	require.NoError(t, err)
	require.NotEmptyf(t, thirdItemRet, "Should not be empty")
	require.Equal(t, []byte(`secondValue`), thirdItemRet.Value, "Should have referenced item value")
}

func TestStoreIndexReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx1, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`item1`)}}})
	require.NoError(t, err)

	idx2, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`item2`)}}})
	require.NoError(t, err)

	ref, err := db.SetReference(&schema.ReferenceRequest{ReferencedKey: []byte(`aaa`), Key: []byte(`myTag1`), AtTx: idx1.Id, BoundRef: true})
	require.NoError(t, err)

	ref, err = db.SetReference(&schema.ReferenceRequest{ReferencedKey: []byte(`aaa`), Key: []byte(`myTag2`), AtTx: idx2.Id, BoundRef: true})
	require.NoError(t, err)

	tag1, err := db.Get(&schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: ref.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag1.Key)
	require.Equal(t, []byte(`item1`), tag1.Value)

	tag2, err := db.Get(&schema.KeyRequest{Key: []byte(`myTag2`), SinceTx: ref.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag2.Key)
	require.Equal(t, []byte(`item2`), tag2.Value)
}

func TestStoreReferenceKeyNotProvided(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	_, err := db.SetReference(&schema.ReferenceRequest{Key: []byte(`myTag1`), AtTx: 123, BoundRef: true})
	require.Equal(t, store.ErrIllegalArguments, err)
}

func TestStore_GetOnReferenceOnSameKeyReturnsAlwaysLastValue(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx1, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`item1`)}}})
	require.NoError(t, err)

	_, err = db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`item2`)}}})
	require.NoError(t, err)

	_, err = db.SetReference(&schema.ReferenceRequest{Key: []byte(`myTag1`), ReferencedKey: []byte(`aaa`)})
	require.NoError(t, err)

	ref, err := db.SetReference(&schema.ReferenceRequest{Key: []byte(`myTag2`), ReferencedKey: []byte(`aaa`), AtTx: idx1.Id, BoundRef: true})
	require.NoError(t, err)

	tag2, err := db.Get(&schema.KeyRequest{Key: []byte(`myTag2`), SinceTx: ref.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag2.Key)
	require.Equal(t, []byte(`item1`), tag2.Value)

	tag1b, err := db.Get(&schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: ref.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag1b.Key)
	require.Equal(t, []byte(`item2`), tag1b.Value)
}

func TestStore_ReferenceIllegalArgument(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.SetReference(nil)
	require.Equal(t, err, store.ErrIllegalArguments)
}

func TestStore_ReferencedItemNotFound(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.SetReference(&schema.ReferenceRequest{ReferencedKey: []byte(`aaa`), Key: []byte(`notExists`)})
	require.Equal(t, store.ErrKeyNotFound, err)
}

func TestStoreVerifiableReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.VerifiableSetReference(nil)
	require.Equal(t, store.ErrIllegalArguments, err)

	req := &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}}
	meta, err := db.Set(req)
	require.NoError(t, err)

	_, err = db.VerifiableSetReference(&schema.VerifiableReferenceRequest{
		ReferenceRequest: nil,
		ProveSinceTx:     meta.Id,
	})
	require.Equal(t, store.ErrIllegalArguments, err)

	refReq := &schema.ReferenceRequest{
		Key:           []byte(`myTag`),
		ReferencedKey: []byte(`firstKey`),
	}

	_, err = db.VerifiableSetReference(&schema.VerifiableReferenceRequest{
		ReferenceRequest: refReq,
		ProveSinceTx:     meta.Id + 1,
	})
	require.Equal(t, store.ErrIllegalArguments, err)

	vtx, err := db.VerifiableSetReference(&schema.VerifiableReferenceRequest{
		ReferenceRequest: refReq,
		ProveSinceTx:     meta.Id,
	})
	require.NoError(t, err)
	require.Equal(t, WrapWithPrefix([]byte(`myTag`), SetKeyPrefix), vtx.Tx.Entries[0].Key)

	dualProof := schema.DualProofFrom(vtx.DualProof)

	verifies := store.VerifyDualProof(
		dualProof,
		meta.Id,
		vtx.Tx.Metadata.Id,
		schema.TxMetadataFrom(meta).Alh(),
		dualProof.TargetTxMetadata.Alh(),
	)
	require.True(t, verifies)

	keyReq := &schema.KeyRequest{Key: []byte(`myTag`), SinceTx: vtx.Tx.Metadata.Id}

	firstItemRet, err := db.Get(keyReq)
	require.NoError(t, err)
	require.Equal(t, []byte(`firstValue`), firstItemRet.Value, "Should have referenced item value")
}
