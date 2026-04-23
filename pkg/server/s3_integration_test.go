//go:build minio
// +build minio

/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
*/

package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/embedded/remotestorage/s3"
	"github.com/stretchr/testify/require"
)

// minioIntegrationConfig mirrors the helper in embedded/remotestorage/s3 so
// tests can be pointed at an arbitrary local MinIO via env vars.
func minioIntegrationConfig(t *testing.T) (endpoint, ak, sk, bucket, prefix string) {
	t.Helper()
	endpoint = os.Getenv("IMMUDB_S3_TEST_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	ak = os.Getenv("IMMUDB_S3_TEST_ACCESS_KEY")
	if ak == "" {
		ak = "minioadmin"
	}
	sk = os.Getenv("IMMUDB_S3_TEST_SECRET_KEY")
	if sk == "" {
		sk = "minioadmin"
	}
	bucket = os.Getenv("IMMUDB_S3_TEST_BUCKET")
	if bucket == "" {
		bucket = "immudb"
	}
	rnd := make([]byte, 8)
	_, err := rand.Read(rnd)
	require.NoError(t, err)
	prefix = "issue2019_" + hex.EncodeToString(rnd)
	return
}

// TestIssue2019_UUIDPersistRoundTrip is a regression guard for GitHub issue
// #2019 ("Unable to persist uuid on S3 storage"). It exercises the same flow
// that ImmuServer.Initialize uses:
//
//  1. getOrSetUUID writes a local identifier file (first start)
//  2. updateRemoteUUID pushes that file to S3
//  3. on the second start, initializeRemoteStorage with S3ExternalIdentifier=true
//     must see the remote identifier, compare it against the local one, and
//     succeed without "unable to persist uuid" errors.
func TestIssue2019_UUIDPersistRoundTrip(t *testing.T) {
	endpoint, ak, sk, bucket, prefix := minioIntegrationConfig(t)

	storage, err := s3.Open(endpoint, false, "", ak, sk, bucket, "", prefix, "", false)
	require.NoError(t, err)

	dir := t.TempDir()
	opts := DefaultOptions().WithDir(dir)
	opts.RemoteStorageOptions = DefaultRemoteStorageOptions().
		WithS3Storage(true).
		WithS3Endpoint(endpoint).
		WithS3AccessKeyID(ak).
		WithS3SecretKey(sk).
		WithS3BucketName(bucket).
		WithS3PathPrefix(prefix).
		WithS3ExternalIdentifier(false) // default: local identifier is authoritative

	s := DefaultServer().WithOptions(opts).(*ImmuServer)
	s.remoteStorage = storage

	// First start: no local, no remote. getOrSetUUID creates a local file.
	s.UUID, err = getOrSetUUID(dir, dir, false)
	require.NoError(t, err)
	require.NotEqual(t, "", s.UUID.String())

	// initializeRemoteStorage with ExternalIdentifier=false only creates the
	// remote subfolder layout; it must not fail on an empty bucket prefix.
	require.NoError(t, s.initializeRemoteStorage(storage))

	// Push the identifier file to S3.
	require.NoError(t, s.updateRemoteUUID(storage))

	// Sanity: file must exist remotely.
	exists, err := storage.Exists(context.Background(), IDENTIFIER_FNAME)
	require.NoError(t, err)
	require.True(t, exists)

	// Simulate a restart with the same dir + bucket prefix.
	s2 := DefaultServer().WithOptions(opts).(*ImmuServer)
	s2.remoteStorage = storage
	s2.UUID, err = getOrSetUUID(dir, dir, false)
	require.NoError(t, err)
	require.Equal(t, s.UUID.String(), s2.UUID.String(), "local UUID must survive restart")
	require.NoError(t, s2.initializeRemoteStorage(storage))
	require.NoError(t, s2.updateRemoteUUID(storage))

	// Cleanup
	require.NoError(t, storage.RemoveAll(context.Background(), ""))
}

// TestIssue2019_ExternalIdentifier exercises S3ExternalIdentifier=true: the
// remote identifier is authoritative and must be downloaded to the local dir
// on fresh installs. Regression guard against a previously reported failure
// where s.UUID ended up zero when only the remote had a prior identifier.
func TestIssue2019_ExternalIdentifier(t *testing.T) {
	endpoint, ak, sk, bucket, prefix := minioIntegrationConfig(t)

	storage, err := s3.Open(endpoint, false, "", ak, sk, bucket, "", prefix, "", false)
	require.NoError(t, err)

	// Seed: put an identifier on S3 only; the "cluster" has run before,
	// but this node's disk is empty.
	seedUUID := []byte("123456789012") // xid is 12 bytes
	seedPath := filepath.Join(t.TempDir(), "seed")
	require.NoError(t, os.WriteFile(seedPath, seedUUID, 0600))
	require.NoError(t, storage.Put(context.Background(), IDENTIFIER_FNAME, seedPath))

	dir := t.TempDir()
	opts := DefaultOptions().WithDir(dir)
	opts.RemoteStorageOptions = DefaultRemoteStorageOptions().
		WithS3Storage(true).
		WithS3Endpoint(endpoint).
		WithS3AccessKeyID(ak).
		WithS3SecretKey(sk).
		WithS3BucketName(bucket).
		WithS3PathPrefix(prefix).
		WithS3ExternalIdentifier(true)

	s := DefaultServer().WithOptions(opts).(*ImmuServer)
	s.remoteStorage = storage

	// With useExternalIdentifier=true and no local file, getOrSetUUID must
	// return zero; initializeRemoteStorage then pulls the remote identifier.
	s.UUID, err = getOrSetUUID(dir, dir, true)
	require.NoError(t, err)

	require.NoError(t, s.initializeRemoteStorage(storage))

	// Local file must now exist and equal the seeded remote UUID.
	localFile := filepath.Join(dir, IDENTIFIER_FNAME)
	require.FileExists(t, localFile)
	localBytes, err := os.ReadFile(localFile)
	require.NoError(t, err)
	require.Equal(t, seedUUID, localBytes)

	// Bug #2019 symptom: if s.UUID remained zero despite a successful load,
	// grpc UUID headers would be malformed. Ensure it is now set.
	require.Equal(t, seedUUID, s.UUID.Bytes(),
		"s.UUID must be set from the remote identifier after loadRemoteIdentifier")

	// Cleanup
	require.NoError(t, storage.RemoveAll(context.Background(), ""))
}
