/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
*/

package s3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithSSE(t *testing.T) {
	newStorage := func(t *testing.T) *Storage {
		t.Helper()
		s, err := Open("http://localhost:9000", false, "", "k", "s", "b", "", "", "", false)
		require.NoError(t, err)
		return s.(*Storage)
	}

	t.Run("empty disables SSE", func(t *testing.T) {
		s := newStorage(t)
		require.NoError(t, s.WithSSE("", ""))
		require.Empty(t, s.sseAlgorithm)
	})
	t.Run("AES256 accepted", func(t *testing.T) {
		s := newStorage(t)
		require.NoError(t, s.WithSSE(SSEAlgorithmAES256, ""))
		require.Equal(t, SSEAlgorithmAES256, s.sseAlgorithm)
	})
	t.Run("aws:kms with key id accepted", func(t *testing.T) {
		s := newStorage(t)
		require.NoError(t, s.WithSSE(SSEAlgorithmKMS, "arn:aws:kms:us-east-1:111:key/abc"))
		require.Equal(t, SSEAlgorithmKMS, s.sseAlgorithm)
		require.Equal(t, "arn:aws:kms:us-east-1:111:key/abc", s.sseKMSKeyID)
	})
	t.Run("unknown algorithm rejected", func(t *testing.T) {
		s := newStorage(t)
		require.ErrorIs(t, s.WithSSE("ROT13", ""), ErrInvalidSSEAlgorithm)
	})
	t.Run("kms key id without aws:kms rejected", func(t *testing.T) {
		s := newStorage(t)
		require.ErrorIs(t, s.WithSSE(SSEAlgorithmAES256, "some-key"), ErrInvalidArguments)
	})
}

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
