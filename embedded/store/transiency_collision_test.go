/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/
*/

package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTransiencyKeyRefCollision pins the root cause of the
// `ErrCannotUpdateKeyTransiency` firing on UPDATE-after-INSERT within a
// single SQL transaction (Gitea repo-create repro).
//
// Before the fix, OngoingTx.entriesByKey[kid] held a keyRef from two
// overlapping integer spaces: non-transient keys used len(entries)-1
// (slice indices starting at 0), and transient keys used
// len(entriesByKey) at insertion (monotonic counter also starting at 0).
// When an SQL INSERT laid down an indexer-walk transient entry at
// keyRef=0 for the mapped primary-key entry, the subsequent main write
// for the non-transient row key also landed at keyRef=0 (len(entries)-1).
// The next write for the main key found transientEntries[0] populated
// and incorrectly concluded the main key had been previously written
// as transient, firing ErrCannotUpdateKeyTransiency.
//
// The fix assigns transient keyRefs in a negative space so they can't
// collide with the non-negative tx.entries[] indices.
func TestTransiencyKeyRefCollision(t *testing.T) {
	st, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)
	defer st.Close()

	// Register an indexer that remaps A->B. Non-transient writes on a
	// key with the source prefix trigger an internal transient write on
	// a target-prefix key, mirroring the SQL primary-row indexer.
	err = st.InitIndexing(&IndexSpec{
		SourcePrefix: []byte("src:"),
		TargetPrefix: []byte("tgt:"),
		TargetEntryMapper: func(k, _ []byte) ([]byte, error) {
			return append([]byte("tgt:"), k[len("src:"):]...), nil
		},
		InjectiveMapping: true,
	})
	require.NoError(t, err)

	tx, err := st.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	srcKey := []byte("src:row1")

	// First non-transient write: primary row insert. The indexer walk
	// stashes an internal transient entry for tgt:row1.
	require.NoError(t, tx.Set(srcKey, nil, []byte("v1")))

	// Second non-transient write on the same primary row key: the
	// UPDATE. Without the fix this fires
	//   ErrCannotUpdateKeyTransiency
	// because the keyRef namespaces collided and transientEntries[0]
	// looked like the prior state for srcKey even though it was really
	// the transient entry created by the indexer walk on a different
	// target key.
	err = tx.Set(srcKey, nil, []byte("v2"))
	require.NoError(t, err,
		"two non-transient Set calls on the same key in one tx must not fire ErrCannotUpdateKeyTransiency — the indexer-walk transient entry lives in its own keyRef space")

	_, err = tx.Commit(context.Background())
	require.NoError(t, err)
}
