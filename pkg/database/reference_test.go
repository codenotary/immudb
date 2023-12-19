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
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestStoreReference(t *testing.T) {
	db := makeDb(t)

	req := &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}}
	txhdr, err := db.Set(context.Background(), req)
	require.NoError(t, err)

	item, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte(`firstKey`), SinceTx: txhdr.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`firstKey`), item.Key)
	require.Equal(t, []byte(`firstValue`), item.Value)

	refOpts := &schema.ReferenceRequest{
		Key:           []byte(`myTag`),
		ReferencedKey: []byte(`secondKey`),
	}
	txhdr, err = db.SetReference(context.Background(), refOpts)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	refOpts = &schema.ReferenceRequest{
		Key:           []byte(`firstKeyR`),
		ReferencedKey: []byte(`firstKey`),
		AtTx:          0,
		BoundRef:      true,
	}
	_, err = db.SetReference(context.Background(), refOpts)
	require.ErrorIs(t, err, store.ErrIllegalArguments)

	refOpts = &schema.ReferenceRequest{
		Key:           []byte(`firstKey`),
		ReferencedKey: []byte(`firstKey`),
	}
	txhdr, err = db.SetReference(context.Background(), refOpts)
	require.ErrorIs(t, err, ErrFinalKeyCannotBeConvertedIntoReference)

	refOpts = &schema.ReferenceRequest{
		Key:           []byte(`myTag`),
		ReferencedKey: []byte(`firstKey`),
	}
	txhdr, err = db.SetReference(context.Background(), refOpts)
	require.NoError(t, err)
	require.Equal(t, uint64(2), txhdr.Id)

	keyReq := &schema.KeyRequest{Key: []byte(`myTag`), SinceTx: txhdr.Id}

	firstItemRet, err := db.Get(context.Background(), keyReq)
	require.NoError(t, err)
	require.Equal(t, []byte(`firstValue`), firstItemRet.Value, "Should have referenced item value")

	vitem, err := db.VerifiableGet(context.Background(), &schema.VerifiableGetRequest{
		KeyRequest:   keyReq,
		ProveSinceTx: 1,
	})
	require.NoError(t, err)
	require.Equal(t, []byte(`firstKey`), vitem.Entry.Key)
	require.Equal(t, []byte(`firstValue`), vitem.Entry.Value)

	inclusionProof := schema.InclusionProofFromProto(vitem.InclusionProof)

	var eh [sha256.Size]byte
	copy(eh[:], vitem.VerifiableTx.Tx.Header.EH)

	entrySpec := EncodeReference([]byte(`myTag`), nil, []byte(`firstKey`), 0)

	entrySpecDigest, err := store.EntrySpecDigestFor(int(txhdr.Version))
	require.NoError(t, err)
	require.NotNil(t, entrySpecDigest)

	verifies := store.VerifyInclusion(
		inclusionProof,
		entrySpecDigest(entrySpec),
		eh,
	)
	require.True(t, verifies)
}

func TestStore_GetReferenceWithIndexResolution(t *testing.T) {
	db := makeDb(t)

	set, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`value1`)}}})
	require.NoError(t, err)

	_, err = db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`value2`)}}})
	require.NoError(t, err)

	ref, err := db.SetReference(context.Background(), &schema.ReferenceRequest{Key: []byte(`myTag1`), ReferencedKey: []byte(`aaa`), AtTx: set.Id, BoundRef: true})
	require.NoError(t, err)

	tag3, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: ref.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag3.Key)
	require.Equal(t, []byte(`value1`), tag3.Value)
}

func TestStoreInvalidReferenceToReference(t *testing.T) {
	db := makeDb(t)

	req := &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}}
	txhdr, err := db.Set(context.Background(), req)
	require.NoError(t, err)

	ref1, err := db.SetReference(context.Background(), &schema.ReferenceRequest{Key: []byte(`myTag1`), ReferencedKey: []byte(`firstKey`), AtTx: txhdr.Id, BoundRef: true})
	require.NoError(t, err)

	_, err = db.Get(context.Background(), &schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: ref1.Id})
	require.NoError(t, err)

	_, err = db.SetReference(context.Background(), &schema.ReferenceRequest{Key: []byte(`myTag2`), ReferencedKey: []byte(`myTag1`)})
	require.ErrorIs(t, err, ErrReferencedKeyCannotBeAReference)
}

func TestStoreReferenceAsyncCommit(t *testing.T) {
	db := makeDb(t)

	firstIndex, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}})
	require.NoError(t, err)

	secondIndex, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`secondKey`), Value: []byte(`secondValue`)}}})
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

		ref, err := db.SetReference(context.Background(), refOpts)
		require.NoError(t, err, "n=%d", n)
		require.Equal(t, n+1+2, ref.Id, "n=%d", n)
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

		item, err := db.Get(context.Background(), &schema.KeyRequest{Key: tag, SinceTx: 67})
		require.NoError(t, err, "n=%d", n)
		require.Equal(t, index, item.Tx, "n=%d", n)
		require.Equal(t, itemVal, item.Value, "n=%d", n)
		require.Equal(t, itemKey, item.Key, "n=%d", n)
	}
}

func TestStoreMultipleReferenceOnSameKey(t *testing.T) {
	db := makeDb(t)

	idx0, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}})
	require.NoError(t, err)

	idx1, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`secondKey`), Value: []byte(`secondValue`)}}})
	require.NoError(t, err)

	refOpts1 := &schema.ReferenceRequest{
		Key:           []byte(`myTag1`),
		ReferencedKey: []byte(`firstKey`),
		AtTx:          idx0.Id,
		BoundRef:      true,
	}

	reference1, err := db.SetReference(context.Background(), refOpts1)
	require.NoError(t, err)
	require.Exactly(t, uint64(3), reference1.Id)
	require.NotEmptyf(t, reference1, "Should not be empty")

	refOpts2 := &schema.ReferenceRequest{
		Key:           []byte(`myTag2`),
		ReferencedKey: []byte(`firstKey`),
		AtTx:          idx0.Id,
		BoundRef:      true,
	}
	reference2, err := db.SetReference(context.Background(), refOpts2)
	require.NoError(t, err)
	require.Exactly(t, uint64(4), reference2.Id)
	require.NotEmptyf(t, reference2, "Should not be empty")

	refOpts3 := &schema.ReferenceRequest{
		Key:           []byte(`myTag3`),
		ReferencedKey: []byte(`secondKey`),
		AtTx:          idx1.Id,
		BoundRef:      true,
	}
	reference3, err := db.SetReference(context.Background(), refOpts3)
	require.NoError(t, err)
	require.Exactly(t, uint64(5), reference3.Id)
	require.NotEmptyf(t, reference3, "Should not be empty")

	firstTagRet, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: reference3.Id})
	require.NoError(t, err)
	require.NotEmptyf(t, firstTagRet, "Should not be empty")
	require.Equal(t, []byte(`firstValue`), firstTagRet.Value, "Should have referenced item value")

	secondTagRet, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte(`myTag2`), SinceTx: reference3.Id})
	require.NoError(t, err)
	require.NotEmptyf(t, secondTagRet, "Should not be empty")
	require.Equal(t, []byte(`firstValue`), secondTagRet.Value, "Should have referenced item value")

	thirdItemRet, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte(`myTag3`), SinceTx: reference3.Id})
	require.NoError(t, err)
	require.NotEmptyf(t, thirdItemRet, "Should not be empty")
	require.Equal(t, []byte(`secondValue`), thirdItemRet.Value, "Should have referenced item value")
}

func TestStoreIndexReference(t *testing.T) {
	db := makeDb(t)

	idx1, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`item1`)}}})
	require.NoError(t, err)

	idx2, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`item2`)}}})
	require.NoError(t, err)

	_, err = db.SetReference(context.Background(), &schema.ReferenceRequest{ReferencedKey: []byte(`aaa`), Key: []byte(`myTag1`), AtTx: idx1.Id, BoundRef: true})
	require.NoError(t, err)

	ref, err := db.SetReference(context.Background(), &schema.ReferenceRequest{ReferencedKey: []byte(`aaa`), Key: []byte(`myTag2`), AtTx: idx2.Id, BoundRef: true})
	require.NoError(t, err)

	tag1, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: ref.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag1.Key)
	require.Equal(t, []byte(`item1`), tag1.Value)

	tag2, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte(`myTag2`), SinceTx: ref.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag2.Key)
	require.Equal(t, []byte(`item2`), tag2.Value)
}

func TestStoreReferenceKeyNotProvided(t *testing.T) {
	db := makeDb(t)
	_, err := db.SetReference(context.Background(), &schema.ReferenceRequest{Key: []byte(`myTag1`), AtTx: 123, BoundRef: true})
	require.ErrorIs(t, err, store.ErrIllegalArguments)
}

func TestStore_GetOnReferenceOnSameKeyReturnsAlwaysLastValue(t *testing.T) {
	db := makeDb(t)

	idx1, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`item1`)}}})
	require.NoError(t, err)

	_, err = db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`item2`)}}})
	require.NoError(t, err)

	_, err = db.SetReference(context.Background(), &schema.ReferenceRequest{Key: []byte(`myTag1`), ReferencedKey: []byte(`aaa`)})
	require.NoError(t, err)

	ref, err := db.SetReference(context.Background(), &schema.ReferenceRequest{Key: []byte(`myTag2`), ReferencedKey: []byte(`aaa`), AtTx: idx1.Id, BoundRef: true})
	require.NoError(t, err)

	tag2, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte(`myTag2`), SinceTx: ref.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag2.Key)
	require.Equal(t, []byte(`item1`), tag2.Value)

	tag1b, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: ref.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag1b.Key)
	require.Equal(t, []byte(`item2`), tag1b.Value)
}

func TestStore_ReferenceIllegalArgument(t *testing.T) {
	db := makeDb(t)

	_, err := db.SetReference(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrIllegalArguments)
}

func TestStore_ReferencedItemNotFound(t *testing.T) {
	db := makeDb(t)

	_, err := db.SetReference(context.Background(), &schema.ReferenceRequest{ReferencedKey: []byte(`aaa`), Key: []byte(`notExists`)})
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestStoreVerifiableReference(t *testing.T) {
	db := makeDb(t)

	_, err := db.VerifiableSetReference(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrIllegalArguments)

	req := &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}}
	txhdr, err := db.Set(context.Background(), req)
	require.NoError(t, err)

	_, err = db.VerifiableSetReference(context.Background(), &schema.VerifiableReferenceRequest{
		ReferenceRequest: nil,
		ProveSinceTx:     txhdr.Id,
	})
	require.ErrorIs(t, err, store.ErrIllegalArguments)

	refReq := &schema.ReferenceRequest{
		Key:           []byte(`myTag`),
		ReferencedKey: []byte(`firstKey`),
	}

	_, err = db.VerifiableSetReference(context.Background(), &schema.VerifiableReferenceRequest{
		ReferenceRequest: refReq,
		ProveSinceTx:     txhdr.Id + 1,
	})
	require.ErrorIs(t, err, store.ErrIllegalArguments)

	vtx, err := db.VerifiableSetReference(context.Background(), &schema.VerifiableReferenceRequest{
		ReferenceRequest: refReq,
		ProveSinceTx:     txhdr.Id,
	})
	require.NoError(t, err)
	require.Equal(t, WrapWithPrefix([]byte(`myTag`), SetKeyPrefix), vtx.Tx.Entries[0].Key)

	dualProof := schema.DualProofFromProto(vtx.DualProof)

	verifies := store.VerifyDualProof(
		dualProof,
		txhdr.Id,
		vtx.Tx.Header.Id,
		schema.TxHeaderFromProto(txhdr).Alh(),
		dualProof.TargetTxHeader.Alh(),
	)
	require.True(t, verifies)

	keyReq := &schema.KeyRequest{Key: []byte(`myTag`), SinceTx: vtx.Tx.Header.Id}

	firstItemRet, err := db.Get(context.Background(), keyReq)
	require.NoError(t, err)
	require.Equal(t, []byte(`firstValue`), firstItemRet.Value, "Should have referenced item value")
}

func TestStoreReferenceWithPreconditions(t *testing.T) {
	db := makeDb(t)

	_, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{
		Key:   []byte("key"),
		Value: []byte("value"),
	}}})
	require.NoError(t, err)

	_, err = db.SetReference(context.Background(), &schema.ReferenceRequest{
		Key:           []byte("reference"),
		ReferencedKey: []byte("key"),
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustExist([]byte("reference")),
		},
	})
	require.ErrorIs(t, err, store.ErrPreconditionFailed)

	_, err = db.Get(context.Background(), &schema.KeyRequest{
		Key: []byte("reference"),
	})
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	_, err = db.SetReference(context.Background(), &schema.ReferenceRequest{
		Key:           []byte("reference"),
		ReferencedKey: []byte("key"),
		Preconditions: []*schema.Precondition{nil},
	})
	require.ErrorIs(t, err, store.ErrInvalidPrecondition)

	_, err = db.SetReference(context.Background(), &schema.ReferenceRequest{
		Key:           []byte("reference"),
		ReferencedKey: []byte("key"),
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustNotExist([]byte("reference-long-key" + strings.Repeat("*", db.GetOptions().storeOpts.MaxKeyLen))),
		},
	})
	require.ErrorIs(t, err, store.ErrInvalidPrecondition)

	c := []*schema.Precondition{}
	for i := 0; i <= db.GetOptions().storeOpts.MaxTxEntries; i++ {
		c = append(c,
			schema.PreconditionKeyMustNotExist([]byte(fmt.Sprintf("key_%d", i))),
		)
	}

	_, err = db.SetReference(context.Background(), &schema.ReferenceRequest{
		Key:           []byte("reference"),
		ReferencedKey: []byte("key"),
		Preconditions: c,
	})
	require.ErrorIs(t, err, store.ErrInvalidPrecondition)
}
