package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOps_ValidateErrDuplicatedKeysNotSupported(t *testing.T) {
	aOps := &Ops{
		Operations: []*Op{
			{
				Operation: &Op_Kv{
					Kv: &KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &Op_Kv{
					Kv: &KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &Op_ZAdd{
					ZAdd: &ZAddRequest{
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

func TestOps_ValidateErrDuplicateZAddNotSupported(t *testing.T) {
	aOps := &Ops{
		Operations: []*Op{
			{
				Operation: &Op_Kv{
					Kv: &KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &Op_ZAdd{
					ZAdd: &ZAddRequest{
						Key: []byte(`key`),
						Score: &Score{
							Score: 5.6,
						},
						AtTx: int64(1),
					},
				},
			},
			{
				Operation: &Op_ZAdd{
					ZAdd: &ZAddRequest{
						Key: []byte(`key`),
						Score: &Score{
							Score: 5.6,
						},
						AtTx: int64(1),
					},
				},
			},
		},
	}
	err := aOps.Validate()
	assert.Equal(t, err, ErrDuplicatedZAddNotSupported)
}

func TestOps_ValidateErrEmptySet(t *testing.T) {
	aOps := &Ops{
		Operations: []*Op{},
	}
	err := aOps.Validate()
	assert.Equal(t, err, ErrEmptySet)
}

func TestOps_ValidateErrDuplicate(t *testing.T) {
	aOps := &Ops{
		Operations: []*Op{
			{
				Operation: &Op_Kv{
					Kv: &KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &Op_ZAdd{
					ZAdd: &ZAddRequest{
						Key: []byte(`key`),
						Score: &Score{
							Score: 5.6,
						},
						AtTx: int64(1),
					},
				},
			},
		},
	}
	err := aOps.Validate()
	assert.NoError(t, err)
}

func TestOps_ValidateUnexpectedType(t *testing.T) {
	aOps := &Ops{
		Operations: []*Op{
			{
				Operation: &Op_Unexpected{},
			},
		},
	}
	err := aOps.Validate()
	assert.Equal(t, status.Error(codes.InvalidArgument, "batch operation has unexpected type *schema.Op_Unexpected"), err)
}
func TestExecAllOpsNilElementFound(t *testing.T) {
	bOps := make([]*Op, 2)
	op := &Op{
		Operation: &Op_ZAdd{
			ZAdd: &ZAddRequest{
				Key: []byte(`key`),
				Score: &Score{
					Score: 5.6,
				},
				AtTx: int64(4),
			},
		},
	}
	bOps[1] = op
	aOps := &Ops{Operations: bOps}
	err := aOps.Validate()
	assert.Equal(t, status.Error(codes.InvalidArgument, "Op is not set"), err)
}

func TestOps_ValidateOperationNilElementFound(t *testing.T) {
	aOps := &Ops{
		Operations: []*Op{
			{
				Operation: nil,
			},
		},
	}
	err := aOps.Validate()
	assert.Equal(t, status.Error(codes.InvalidArgument, "operation is not set"), err)
}
