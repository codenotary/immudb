package schema

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBatchOps_ValidateErrDuplicatedKeysNotSupported(t *testing.T) {
	aOps := &BatchOps{
		Operations: []*BatchOp{
			{
				Operation: &BatchOp_KVs{
					KVs: &KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &BatchOp_KVs{
					KVs: &KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &BatchOp_ZOpts{
					ZOpts: &ZAddOptions{
						Key: []byte(`key`),
						Score: &Score{
							Score: 5.6,
						},
					},
				},
			},
		},
	}
	err := aOps.Validate()
	assert.Equal(t, err, ErrDuplicatedKeysNotSupported)

}

func TestBatchOps_ValidateErrDuplicateZAddNotSupported(t *testing.T) {
	aOps := &BatchOps{
		Operations: []*BatchOp{
			{
				Operation: &BatchOp_KVs{
					KVs: &KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &BatchOp_ZOpts{
					ZOpts: &ZAddOptions{
						Key: []byte(`key`),
						Score: &Score{
							Score: 5.6,
						},
						Index: &Index{
							Index: uint64(1),
						},
					},
				},
			},
			{
				Operation: &BatchOp_ZOpts{
					ZOpts: &ZAddOptions{
						Key: []byte(`key`),
						Score: &Score{
							Score: 5.6,
						},
						Index: &Index{
							Index: uint64(1),
						},
					},
				},
			},
		},
	}
	err := aOps.Validate()
	assert.Equal(t, err, ErrDuplicatedZAddNotSupported)
}

func TestBatchOps_ValidateErrEmptySet(t *testing.T) {
	aOps := &BatchOps{
		Operations: []*BatchOp{},
	}
	err := aOps.Validate()
	assert.Equal(t, err, ErrEmptySet)
}

func TestBatchOps_ValidateErrDuplicate(t *testing.T) {
	aOps := &BatchOps{
		Operations: []*BatchOp{
			{
				Operation: &BatchOp_KVs{
					KVs: &KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &BatchOp_ZOpts{
					ZOpts: &ZAddOptions{
						Key: []byte(`key`),
						Score: &Score{
							Score: 5.6,
						},
						Index: &Index{
							Index: uint64(1),
						},
					},
				},
			},
		},
	}
	err := aOps.Validate()
	assert.NoError(t, err)
}
