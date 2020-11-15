package store

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestSetBatch(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	batchSize := 100

	for b := 0; b < 10; b++ {
		kvList := make([]*schema.KeyValue, batchSize)

		for i := 0; i < batchSize; i++ {
			key := []byte(strconv.FormatUint(uint64(i), 10))
			value := []byte(strconv.FormatUint(uint64(b*batchSize+batchSize+i), 10))
			kvList[i] = &schema.KeyValue{
				Key:   key,
				Value: value,
			}
		}

		index, err := st.SetBatch(schema.KVList{KVs: kvList})
		assert.NoError(t, err)
		assert.Equal(t, uint64((b+1)*batchSize), index.GetIndex()+1)

		for i := 0; i < batchSize; i++ {
			key := []byte(strconv.FormatUint(uint64(i), 10))
			value := []byte(strconv.FormatUint(uint64(b*batchSize+batchSize+i), 10))
			item, err := st.Get(schema.Key{Key: key})
			assert.NoError(t, err)
			assert.Equal(t, value, item.Value)
			assert.Equal(t, uint64(b*batchSize+i), item.Index)

			safeItem, err := st.SafeGet(schema.SafeGetOptions{Key: key}) //no prev root
			assert.NoError(t, err)
			assert.Equal(t, key, safeItem.Item.Key)
			assert.Equal(t, value, safeItem.Item.Value)
			assert.Equal(t, item.Index, safeItem.Item.Index)
			assert.True(t, safeItem.Proof.Verify(
				safeItem.Item.Hash(),
				schema.Root{Payload: &schema.RootIndex{}}, // zerovalue signals no prev root
			))
		}
	}
}
