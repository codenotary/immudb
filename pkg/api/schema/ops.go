/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package schema

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *ExecAllRequest) Validate() error {
	if len(m.GetOperations()) == 0 {
		return ErrEmptySet
	}
	mops := make(map[[sha256.Size]byte]struct{}, len(m.GetOperations()))

	for _, op := range m.Operations {
		if op == nil {
			return status.New(codes.InvalidArgument, "Op is not set").Err()
		}
		switch x := op.Operation.(type) {
		case *Op_Kv:
			mk := sha256.Sum256(x.Kv.Key)
			if _, ok := mops[mk]; ok {
				return fmt.Errorf("%w: key/reference '%s'", ErrDuplicatedKeysNotSupported, x.Kv.Key)
			}
			mops[mk] = struct{}{}
		case *Op_ZAdd:
			mk := sha256.Sum256(bytes.Join([][]byte{x.ZAdd.Set, x.ZAdd.Key, []byte(strconv.FormatUint(x.ZAdd.AtTx, 10))}, nil))
			if _, ok := mops[mk]; ok {
				return ErrDuplicatedZAddNotSupported
			}
			mops[mk] = struct{}{}
		case *Op_Ref:
			mk := sha256.Sum256(x.Ref.Key)
			if _, ok := mops[mk]; ok {
				return fmt.Errorf("%w: key/reference '%s'", ErrDuplicatedKeysNotSupported, x.Ref.Key)
			}
			mops[mk] = struct{}{}

			mk = sha256.Sum256(bytes.Join([][]byte{x.Ref.Key, x.Ref.ReferencedKey, []byte(strconv.FormatUint(x.Ref.AtTx, 10))}, nil))
			if _, ok := mops[mk]; ok {
				return ErrDuplicatedReferencesNotSupported
			}
			mops[mk] = struct{}{}
		case nil:
			return status.New(codes.InvalidArgument, "operation is not set").Err()
		default:
			return status.Newf(codes.InvalidArgument, "unexpected type %T", x).Err()
		}
	}

	return nil
}
