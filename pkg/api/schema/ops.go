package schema

import (
	"bytes"
	"crypto/sha256"
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
		case *Op_KVs:
			mk := sha256.Sum256(x.KVs.Key)
			if _, ok := mops[mk]; ok {
				return ErrDuplicatedKeysNotSupported
			}
			mops[mk] = struct{}{}
		case *Op_ZOpts:
			mk := sha256.Sum256(bytes.Join([][]byte{x.ZOpts.Set, x.ZOpts.Key, []byte(x.ZOpts.Index.String())}, nil))
			if _, ok := mops[mk]; ok {
				return ErrDuplicatedZAddNotSupported
			}
			mops[mk] = struct{}{}
		case *Op_ROpts:
			mk := sha256.Sum256(bytes.Join([][]byte{x.ROpts.Reference, x.ROpts.Key, []byte(x.ROpts.Index.String())}, nil))
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
