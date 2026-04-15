/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/
*/

package bmessages

import (
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/stretchr/testify/require"
)

// TestRenderValueAsByte_BlobUsesHexPrefix pins the PG-canonical text
// format for BYTEA values: `\x<hex>`. Without the prefix, lib/pq and
// pgx treat the response as escape-format and return raw hex characters
// to the application, breaking Gitea's YAML-decoding of workflow_payload
// and the PushCommits hook's `hex.DecodeString` on action content.
func TestRenderValueAsByte_BlobUsesHexPrefix(t *testing.T) {
	// Bytes that intentionally include 0x5c (backslash) so a regression
	// to the pre-fix "raw hex" path would produce a `\` inside the
	// response — which is what fired Gitea's
	//   encoding/hex: invalid byte: U+005C '\'
	// crash on PushCommits.
	raw := []byte{0x7b, 0x5c, 0x22, 0x79, 0x65, 0x73, 0x22, 0x7d}
	v := sql.NewBlob(raw)
	got := string(renderValueAsByte(v))
	require.Equal(t, `\x7b5c2279657322`+`7d`, got)
}

func TestRenderValueAsByte_NilBlob(t *testing.T) {
	v := sql.NewNull(sql.BLOBType)
	require.Nil(t, renderValueAsByte(v))
}
