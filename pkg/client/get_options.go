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

package client

import (
	"github.com/codenotary/immudb/pkg/api/schema"
)

// GetOption is used to set additional options when reading a value with a Get call
type GetOption func(req *schema.KeyRequest) error

// NoWait option set to true means that the server should not wait for the indexer
// to be up-to-date with the most recent transaction.
//
// In practice this means that the user will get the result instantly without the
// risk of the server wait for the indexer that may happen in case of a large spike
// of new transactions.
//
// The disadvantage of using this option is that the server can reply with an older
// value for the same key or with a key not found result even though already committed
// transactions contain newer updates to that key.
func NoWait(nowait bool) GetOption {
	return func(req *schema.KeyRequest) error {
		req.NoWait = nowait
		return nil
	}
}

// SinceTx option can be used to avoid waiting for the indexer to be up-to-date
// with the most recent transaction. The client requires though that at least
// the transaction with id given in the tx parameter must be indexed.
//
// This option can be used to avoid additional latency while the server waits
// for the indexer to finish indexing but still guarantees that specific portion
// of the database history has already been indexed.
func SinceTx(tx uint64) GetOption {
	return func(req *schema.KeyRequest) error {
		req.SinceTx = tx
		return nil
	}
}

// AtTx option is used to specify that the value read must be the one set
// at transaction with id given in the tx parameter.
//
// If the key was not modified at given transaction, the request will
// return key not found result even if the key was changed before that transaction.
//
// Using AtTx also allows reading entries set with disabled indexing.
func AtTx(tx uint64) GetOption {
	return func(req *schema.KeyRequest) error {
		req.AtTx = tx
		return nil
	}
}

// AtRevision request specific revision for given key.
//
// Key revision is an integer value that starts at 1 when
// the key is created and then increased by 1 on every update
// made to that key.
//
// The way rev is interpreted depends on the value:
//   - if rev = 0, returns current value
//   - if rev > 0, returns nth revision value,
//     e.g. 1 is the first value, 2 is the second and so on
//   - if rev < 0, returns nth revision value from the end,
//     e.g. -1 is the previous value, -2 is the one before and so on
func AtRevision(rev int64) GetOption {
	return func(req *schema.KeyRequest) error {
		req.AtRevision = rev
		return nil
	}
}
