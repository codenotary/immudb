/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package database

import (
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

func PreconditionFromProto(c *schema.Precondition) (store.Precondition, error) {
	if c == nil {
		return nil, store.ErrInvalidPreconditionNull
	}

	switch c := c.Precondition.(type) {
	case *schema.Precondition_KeyMustExist:
		key := c.KeyMustExist.GetKey()
		if len(key) == 0 {
			return nil, store.ErrInvalidPreconditionNullKey
		}

		return &store.PreconditionKeyMustExist{
			Key: EncodeKey(key),
		}, nil

	case *schema.Precondition_KeyMustNotExist:
		key := c.KeyMustNotExist.GetKey()
		if len(key) == 0 {
			return nil, store.ErrInvalidPreconditionNullKey
		}

		return &store.PreconditionKeyMustNotExist{
			Key: EncodeKey(key),
		}, nil

	case *schema.Precondition_KeyNotModifiedAfterTX:
		key := c.KeyNotModifiedAfterTX.GetKey()
		if len(key) == 0 {
			return nil, store.ErrInvalidPreconditionNullKey
		}
		return &store.PreconditionKeyNotModifiedAfterTx{
			Key:  EncodeKey(key),
			TxID: c.KeyNotModifiedAfterTX.GetTxID(),
		}, nil
	}

	return nil, store.ErrInvalidPreconditionNull
}
