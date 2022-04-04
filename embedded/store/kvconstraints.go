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
