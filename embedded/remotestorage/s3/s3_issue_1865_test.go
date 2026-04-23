//go:build minio
// +build minio

/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
*/

package s3

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/stretchr/testify/require"
)

// TestIssue1865_MultiTenantPrefixIsolation is a regression guard for GitHub
// issue #1865 ("Instead of checking subfolders of the remote storage immudb
// checks it from the root"). Two storages sharing one bucket under distinct
// path prefixes must only see their own objects on ListEntries. Current code
// passes; this test exists to catch future regressions in prefix scoping.
func TestIssue1865_MultiTenantPrefixIsolation(t *testing.T) {
	endpoint, ak, sk, bucket := minioTestConfig()

	rnd := make([]byte, 8)
	_, err := rand.Read(rnd)
	require.NoError(t, err)
	base := fmt.Sprintf("i1865_%x", rnd)

	tenantA, err := Open(endpoint, false, "", ak, sk, bucket, "", base+"/tenantA", "", false)
	require.NoError(t, err)
	tenantB, err := Open(endpoint, false, "", ak, sk, bucket, "", base+"/tenantB", "", false)
	require.NoError(t, err)

	ctx := context.Background()

	putHello := func(s remotestorage.Storage, name, body string) {
		fl, err := ioutil.TempFile("", "")
		require.NoError(t, err)
		fmt.Fprint(fl, body)
		fl.Close()
		defer os.Remove(fl.Name())
		require.NoError(t, s.Put(ctx, name, fl.Name()))
	}

	putHello(tenantA, "sub/a1", "A1")
	putHello(tenantA, "sub/a2", "A2")
	putHello(tenantB, "sub/b1", "B1")

	// Tenant A must see its own two entries only.
	entries, _, err := tenantA.ListEntries(ctx, "sub/")
	require.NoError(t, err)
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	require.ElementsMatch(t, []string{"a1", "a2"}, names,
		"tenantA must not see tenantB's objects; listing leaked across path prefixes")

	// Tenant B must see its own single entry.
	entries, _, err = tenantB.ListEntries(ctx, "sub/")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "b1", entries[0].Name)

	// Cleanup
	_ = tenantA.RemoveAll(ctx, "")
	_ = tenantB.RemoveAll(ctx, "")
}

// TestIssue1865_RootVsPrefixedList verifies the essential invariant from #1865:
// when a storage instance is configured with a non-empty prefix, ListEntries("")
// and ListEntries("sub/") must be scoped to the prefix, not the bucket root.
func TestIssue1865_RootVsPrefixedList(t *testing.T) {
	endpoint, ak, sk, bucket := minioTestConfig()

	rnd := make([]byte, 8)
	_, err := rand.Read(rnd)
	require.NoError(t, err)
	base := fmt.Sprintf("i1865root_%x", rnd)

	// Write an object directly at the bucket root through a no-prefix storage.
	root, err := Open(endpoint, false, "", ak, sk, bucket, "", "", "", false)
	require.NoError(t, err)
	ctx := context.Background()

	noise := base + "_noise_at_root"
	fl, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	fmt.Fprint(fl, "noise")
	fl.Close()
	defer os.Remove(fl.Name())
	require.NoError(t, root.Put(ctx, noise, fl.Name()))
	defer root.Remove(ctx, noise)

	// Now create a prefixed storage and verify its listing does not see root noise.
	tenant, err := Open(endpoint, false, "", ak, sk, bucket, "", base+"/tenant", "", false)
	require.NoError(t, err)

	entries, sub, err := tenant.ListEntries(ctx, "")
	require.NoError(t, err)
	for _, e := range entries {
		require.NotContains(t, e.Name, noise, "prefixed list must not see root-level objects")
	}
	for _, p := range sub {
		require.NotEqual(t, noise, p)
	}
}
