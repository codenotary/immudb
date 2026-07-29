/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

package store_test

import (
	"context"
	"fmt"
	"os"

	"github.com/codenotary/immudb/embedded/store"
)

// ExampleOpen embeds the immudb key-value store directly in-process: no server
// and no container, just a local data directory opened as a library.
func ExampleOpen() {
	dir, err := os.MkdirTemp("", "immudb-embedded-kv")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	st, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		panic(err)
	}
	defer st.Close()

	ctx := context.Background()

	// write a key within a transaction
	tx, err := st.NewWriteOnlyTx(ctx)
	if err != nil {
		panic(err)
	}
	if err := tx.Set([]byte("hello"), nil, []byte("immutable world")); err != nil {
		panic(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		panic(err)
	}

	// read it back from the index
	valRef, err := st.Get(ctx, []byte("hello"))
	if err != nil {
		panic(err)
	}
	val, err := valRef.Resolve()
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s\n", val)

	// Output: immutable world
}

// ExampleVerifyDualProof shows immudb's tamper-evidence: after committing two
// transactions, a dual proof cryptographically links them, and VerifyDualProof
// checks that link with no server involved. The same proof primitives back the
// verifiable client of the full immudb server.
func ExampleVerifyDualProof() {
	dir, err := os.MkdirTemp("", "immudb-embedded-proof")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	st, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		panic(err)
	}
	defer st.Close()

	ctx := context.Background()

	commit := func(key, value []byte) *store.TxHeader {
		tx, err := st.NewWriteOnlyTx(ctx)
		if err != nil {
			panic(err)
		}
		if err := tx.Set(key, nil, value); err != nil {
			panic(err)
		}
		hdr, err := tx.Commit(ctx)
		if err != nil {
			panic(err)
		}
		return hdr
	}

	sourceHdr := commit([]byte("k1"), []byte("v1"))
	targetHdr := commit([]byte("k2"), []byte("v2"))

	// generate a proof that the store's state at targetHdr is consistent with,
	// and includes, the state at sourceHdr
	proof, err := st.DualProof(sourceHdr, targetHdr)
	if err != nil {
		panic(err)
	}

	verified := store.VerifyDualProof(
		proof,
		sourceHdr.ID, targetHdr.ID,
		sourceHdr.Alh(), targetHdr.Alh(),
	)

	fmt.Println(verified)

	// Output: true
}
