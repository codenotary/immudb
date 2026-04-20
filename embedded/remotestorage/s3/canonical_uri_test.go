/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
*/

package s3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestS3CanonicalURI(t *testing.T) {
	for _, tc := range []struct {
		in, out string
	}{
		{"", "/"},
		{"/", "/"},
		{"/foo", "/foo"},
		{"/foo/bar", "/foo/bar"},
		{"/foo/bar.txt", "/foo/bar.txt"},
		{"/bucket/with space.txt", "/bucket/with%20space.txt"},
		{"/bucket/with+plus.txt", "/bucket/with%2Bplus.txt"},
		{"/bucket/with%already.txt", "/bucket/with%25already.txt"},
		{"/bucket/unicode/café.txt", "/bucket/unicode/caf%C3%A9.txt"},
		{"/bucket/a&b=c.txt", "/bucket/a%26b%3Dc.txt"},
		{"/bucket/~tilde_-dot.", "/bucket/~tilde_-dot."},
		// Preserves '/' separator; does not double-encode existing slashes.
		{"/a/b/c", "/a/b/c"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			require.Equal(t, tc.out, s3CanonicalURI(tc.in))
		})
	}
}
