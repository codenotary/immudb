package schema

import (
	"bytes"
	"crypto/sha256"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *Ops) Validate() error {
	if len(m.GetOperations()) == 0 {
		return ErrEmptySet
	}
	mops := make(map[[32]byte]struct{}, len(m.GetOperations()))

	for _, op := range m.Operations {
		if op == nil {
			return status.New(codes.InvalidArgument, "Op is not set").Err()
		}
		switch x := op.Operation.(type) {
		case *Op_Kv:
			mk := sha256.Sum256(x.Kv.Key)
			if _, ok := mops[mk]; ok {
				return ErrDuplicatedKeysNotSupported
			}
			mops[mk] = struct{}{}
		case *Op_ZAdd:
			mk := sha256.Sum256(bytes.Join([][]byte{x.ZAdd.Set, x.ZAdd.Key, []byte(strconv.FormatInt(x.ZAdd.AtTx, 10))}, nil))
			if _, ok := mops[mk]; ok {
				return ErrDuplicatedZAddNotSupported
			}
			mops[mk] = struct{}{}
		case *Op_Ref:
			mk := sha256.Sum256(bytes.Join([][]byte{x.Ref.Reference, x.Ref.Key, []byte(strconv.FormatInt(x.Ref.AtTx, 10))}, nil))
			if _, ok := mops[mk]; ok {
				return ErrDuplicatedReferencesNotSupported
			}
			mops[mk] = struct{}{}
		case nil:
			return status.New(codes.InvalidArgument, "operation is not set").Err()
		default:
			return status.Newf(codes.InvalidArgument, "batch operation has unexpected type %T", x).Err()
		}
	}

	return nil
}
