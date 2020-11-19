package schema

import (
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestBatchOps_ValidateUnexpectedType(t *testing.T) {
	aOps := &BatchOps{
		Operations: []*BatchOp{
			{
				Operation: &Op_Unexpected{},
			},
		},
	}
	err := aOps.Validate()
	assert.Equal(t, status.Error(codes.InvalidArgument, "batch operation has unexpected type *schema.Op_Unexpected"), err)
}
func TestSetBatchOpsNilElementFound(t *testing.T) {
	bOps := make([]*BatchOp, 2)
	op := &BatchOp{
		Operation: &BatchOp_ZOpts{
			ZOpts: &ZAddOptions{
				Key: []byte(`key`),
				Score: &Score{
					Score: 5.6,
				},
				Index: &Index{
					Index: 4,
				},
			},
		},
	}
	bOps[1] = op
	aOps := &BatchOps{Operations: bOps}
	err := aOps.Validate()
	assert.Equal(t, status.Error(codes.InvalidArgument, "batchOp is not set"), err)
}

func TestBatchOps_ValidateOperationNilElementFound(t *testing.T) {
	aOps := &BatchOps{
		Operations: []*BatchOp{
			{
				Operation: nil,
			},
		},
	}
	err := aOps.Validate()
	assert.Equal(t, status.Error(codes.InvalidArgument, "operation is not set"), err)
}
