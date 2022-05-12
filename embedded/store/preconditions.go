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

type Precondition interface {
	String() string

	// Validate performs initial validation check to discard invalid preconditions before even executing them
	Validate(st *ImmuStore) error

	// Check performs the validation on a current state of the database
	Check(idx KeyIndex) (bool, error)
}

type PreconditionKeyMustExist struct {
	Key []byte
}

func (cs *PreconditionKeyMustExist) String() string { return "KeyMustExist" }

func (cs *PreconditionKeyMustExist) Validate(st *ImmuStore) error {
	if len(cs.Key) == 0 {
		return ErrInvalidPreconditionNullKey
	}

	if len(cs.Key) > st.maxKeyLen {
		return ErrInvalidPreconditionMaxKeyLenExceeded
	}

	return nil
}

func (cs *PreconditionKeyMustExist) Check(idx KeyIndex) (bool, error) {
	_, err := idx.Get(cs.Key)
	if err != nil && !errors.Is(err, tbtree.ErrKeyNotFound) {
		return false, err
	}

	return err == nil, nil
}

type PreconditionKeyMustNotExist struct {
	Key []byte
}

func (cs *PreconditionKeyMustNotExist) String() string { return "KeyMustNotExist" }

func (cs *PreconditionKeyMustNotExist) Validate(st *ImmuStore) error {
	if len(cs.Key) == 0 {
		return ErrInvalidPreconditionNullKey
	}

	if len(cs.Key) > st.maxKeyLen {
		return ErrInvalidPreconditionMaxKeyLenExceeded
	}

	return nil
}

func (cs *PreconditionKeyMustNotExist) Check(idx KeyIndex) (bool, error) {
	_, err := idx.Get(cs.Key)
	if err != nil && !errors.Is(err, tbtree.ErrKeyNotFound) {
		return false, err
	}

	return err != nil, nil
}

type PreconditionKeyNotModifiedAfterTx struct {
	Key  []byte
	TxID uint64
}

func (cs *PreconditionKeyNotModifiedAfterTx) String() string { return "KeyNotModifiedAfterTxID" }

func (cs *PreconditionKeyNotModifiedAfterTx) Validate(st *ImmuStore) error {
	if len(cs.Key) == 0 {
		return ErrInvalidPreconditionNullKey
	}

	if len(cs.Key) > st.maxKeyLen {
		return ErrInvalidPreconditionMaxKeyLenExceeded
	}

	if cs.TxID == 0 {
		return ErrInvalidPreconditionInvalidTxID
	}

	return nil
}

func (cs *PreconditionKeyNotModifiedAfterTx) Check(idx KeyIndex) (bool, error) {
	// get the latest entry (it could be deleted or even expired)
	valRef, err := idx.GetWith(cs.Key)
	if err != nil && errors.Is(err, ErrKeyNotFound) {
		// key does not exist thus not modified at all
		return true, nil
	}
	if err != nil {
		return false, err
	}

	return valRef.Tx() <= cs.TxID, nil
}
