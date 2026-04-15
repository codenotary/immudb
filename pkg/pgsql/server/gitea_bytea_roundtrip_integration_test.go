/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/
*/

package server_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGiteaCompat_ByteaRoundtrip pins the BYTEA text-format round-trip
// that Gitea's pq driver relies on for every workflow_payload /
// session.data / dbfs_data blob read. Before the `\x<hex>` prefix fix,
// a BYTEA written in via text-mode INSERT came back as the ASCII of
// the hex encoding, which Gitea then tried to YAML-decode or
// hex-decode, blowing up with
//   yaml: unmarshal errors
//   encoding/hex: invalid byte: U+005C '\'
// on any payload that happened to contain a backslash.
func TestGiteaCompat_ByteaRoundtrip(t *testing.T) {
	_, port := setupTestServer(t)

	db, err := sql.Open("postgres",
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE blob_rt (id INTEGER NOT NULL AUTO_INCREMENT, data BLOB, PRIMARY KEY(id))`)
	require.NoError(t, err)

	// Binary payload that includes the bytes most likely to trip a
	// broken text-format BYTEA path: 0x00 (NUL), 0x5c (backslash),
	// 0x27 (quote), 0x7b/0x7d (braces — typical YAML/JSON delimiters),
	// plus enough bytes to stress length handling.
	want := make([]byte, 0, 256)
	want = append(want, []byte(`{"key":"value\"with\"quotes"}`)...)
	want = append(want, 0x00, 0x5c, 0x27, 0x22, 0x7b, 0x7d, 0xff, 0xfe)
	for i := 0; i < 16; i++ {
		want = append(want, byte(i))
	}

	_, err = db.Exec(`INSERT INTO blob_rt (data) VALUES ($1)`, want)
	require.NoError(t, err)

	var got []byte
	err = db.QueryRow(`SELECT data FROM blob_rt WHERE id = 1`).Scan(&got)
	require.NoError(t, err, "SELECT of a BYTEA column must not error — this is the Gitea workflow_payload / session.data regression guard")
	require.True(t, bytes.Equal(want, got),
		"BYTEA round-trip must preserve bytes exactly\n  want: %x (%d bytes)\n  got : %x (%d bytes)",
		want, len(want), got, len(got))
}
