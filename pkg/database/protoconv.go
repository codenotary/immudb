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
package database

import (
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

func WriteConstraintsFromProto(c *schema.WriteConstraint) (store.WriteConstraint, error) {
	if c == nil {
		return nil, store.ErrInvalidConstraintsNull
	}

	switch c := c.Constraint.(type) {
	case *schema.WriteConstraint_KeyMustExist:
		key := c.KeyMustExist.GetKey()
		if len(key) == 0 {
			return nil, store.ErrInvalidConstraintsNullKey
		}

		return &store.WriteContraintKeyMustExist{
			Key: EncodeKey(key),
		}, nil

	case *schema.WriteConstraint_KeyMustNotExist:
		key := c.KeyMustNotExist.GetKey()
		if len(key) == 0 {
			return nil, store.ErrInvalidConstraintsNullKey
		}

		return &store.WriteContraintKeyMustNotExist{
			Key: EncodeKey(key),
		}, nil

	case *schema.WriteConstraint_KeyNotModifiedAfterTX:
		key := c.KeyNotModifiedAfterTX.GetKey()
		if len(key) == 0 {
			return nil, store.ErrInvalidConstraintsNullKey
		}
		return &store.WriteContraintKeyNotModifiedAfterTx{
			Key:  EncodeKey(key),
			TxID: c.KeyNotModifiedAfterTX.GetTxID(),
		}, nil
	}

	return nil, store.ErrInvalidConstraintsNull
}
