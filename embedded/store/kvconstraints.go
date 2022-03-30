package store

import (
	"errors"
	"fmt"

	"github.com/codenotary/immudb/embedded/tbtree"
)

type KVConstraints struct {
	Key                []byte
	MustExist          bool
	MustNotExist       bool
	NotModifiedAfterTX uint64
}

func (cs *KVConstraints) check(idx *indexer) error {
	_, tx, _, err := idx.Get(cs.Key)
	if err != nil && !errors.Is(err, tbtree.ErrKeyNotFound) {
		return fmt.Errorf("couldn't check KV constraint: %w", err)
	}

	if cs.MustExist && err != nil {
		return fmt.Errorf("%w: key does not exist", ErrConstraintFailed)
	}

	if cs.MustNotExist && err == nil {
		return fmt.Errorf("%w: key already exists", ErrConstraintFailed)
	}

	if cs.NotModifiedAfterTX > 0 && tx > cs.NotModifiedAfterTX {
		return fmt.Errorf("%w: key modified after given TX", ErrConstraintFailed)
	}

	return nil
}
