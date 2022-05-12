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
package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOps_ValidateErrDuplicatedKeysNotSupported(t *testing.T) {
	aOps := &ExecAllRequest{
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
						Key:   []byte(`key`),
						Score: 5.6,
					},
				},
			},
		},
	}
	err := aOps.Validate()
	require.ErrorIs(t, err, ErrDuplicatedKeysNotSupported)

}

func TestOps_ValidateErrDuplicateZAddNotSupported(t *testing.T) {
	aOps := &ExecAllRequest{
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
						Key:   []byte(`key`),
						Score: 5.6,
						AtTx:  1,
					},
				},
			},
			{
				Operation: &Op_ZAdd{
					ZAdd: &ZAddRequest{
						Key:   []byte(`key`),
						Score: 5.6,
						AtTx:  1,
					},
				},
			},
		},
	}
	err := aOps.Validate()
	require.Equal(t, err, ErrDuplicatedZAddNotSupported)
}

func TestOps_ValidateErrEmptySet(t *testing.T) {
	aOps := &ExecAllRequest{
		Operations: []*Op{},
	}
	err := aOps.Validate()
	require.Equal(t, err, ErrEmptySet)
}

func TestOps_ValidateErrDuplicate(t *testing.T) {
	aOps := &ExecAllRequest{
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
						Key:   []byte(`key`),
						Score: 5.6,
						AtTx:  1,
					},
				},
			},
		},
	}
	err := aOps.Validate()
	require.NoError(t, err)
}

func TestOps_ValidateUnexpectedType(t *testing.T) {
	aOps := &ExecAllRequest{
		Operations: []*Op{
			{
				Operation: &Op_Unexpected{},
			},
		},
	}
	err := aOps.Validate()
	require.Error(t, err)
}

func TestExecAllOpsNilElementFound(t *testing.T) {
	bOps := make([]*Op, 2)
	op := &Op{
		Operation: &Op_ZAdd{
			ZAdd: &ZAddRequest{
				Key:   []byte(`key`),
				Score: 5.6,
				AtTx:  4,
			},
		},
	}
	bOps[1] = op
	aOps := &ExecAllRequest{Operations: bOps}
	err := aOps.Validate()
	require.Equal(t, status.Error(codes.InvalidArgument, "Op is not set"), err)
}

func TestOps_ValidateOperationNilElementFound(t *testing.T) {
	aOps := &ExecAllRequest{
		Operations: []*Op{
			{
				Operation: nil,
			},
		},
	}
	err := aOps.Validate()
	require.Equal(t, status.Error(codes.InvalidArgument, "operation is not set"), err)
}
