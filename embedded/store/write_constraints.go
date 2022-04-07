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
package store

import (
	"errors"

	"github.com/codenotary/immudb/embedded/tbtree"
)

type WriteConstraint interface {
	String() string

	// Validate performs initial validation check to discard invalid constraints before even executing them
	Validate(st *ImmuStore) error

	// Check performs the validation on a current state of the database
	Check(idx *indexer) (bool, error)
}

type WriteContraintKeyMustExist struct {
	Key []byte
}

func (cs *WriteContraintKeyMustExist) String() string { return "KeyMustExist" }

func (cs *WriteContraintKeyMustExist) Validate(st *ImmuStore) error {
	if len(cs.Key) == 0 {
		return ErrInvalidConstraintsNullKey
	}

	if len(cs.Key) > st.maxKeyLen {
		return ErrInvalidConstraintsMaxKeyLenExceeded
	}

	return nil
}

func (cs *WriteContraintKeyMustExist) Check(idx *indexer) (bool, error) {
	_, _, _, err := idx.Get(cs.Key)
	if err != nil && !errors.Is(err, tbtree.ErrKeyNotFound) {
		return false, err
	}

	return err == nil, nil
}

type WriteContraintKeyMustNotExist struct {
	Key []byte
}

func (cs *WriteContraintKeyMustNotExist) String() string { return "KeyMustNotExist" }

func (cs *WriteContraintKeyMustNotExist) Validate(st *ImmuStore) error {
	if len(cs.Key) == 0 {
		return ErrInvalidConstraintsNullKey
	}

	if len(cs.Key) > st.maxKeyLen {
		return ErrInvalidConstraintsMaxKeyLenExceeded
	}

	return nil
}

func (cs *WriteContraintKeyMustNotExist) Check(idx *indexer) (bool, error) {
	_, _, _, err := idx.Get(cs.Key)
	if err != nil && !errors.Is(err, tbtree.ErrKeyNotFound) {
		return false, err
	}

	return err != nil, nil
}

type WriteContraintKeyNotModifiedAfterTx struct {
	Key  []byte
	TxID uint64
}

func (cs *WriteContraintKeyNotModifiedAfterTx) String() string { return "KeyNotModifiedAfterTxID" }

func (cs *WriteContraintKeyNotModifiedAfterTx) Validate(st *ImmuStore) error {
	if len(cs.Key) == 0 {
		return ErrInvalidConstraintsNullKey
	}

	if len(cs.Key) > st.maxKeyLen {
		return ErrInvalidConstraintsMaxKeyLenExceeded
	}

	if cs.TxID == 0 {
		return ErrInvalidConstraintsInvalidTxID
	}

	return nil
}

func (cs *WriteContraintKeyNotModifiedAfterTx) Check(idx *indexer) (bool, error) {
	_, tx, _, err := idx.Get(cs.Key)
	if err != nil && !errors.Is(err, tbtree.ErrKeyNotFound) {
		return false, err
	}

	if err != nil {
		// Key does not exist thus not modified at all
		return true, nil
	}

	return tx <= cs.TxID, nil
}
