/*
Copyright 2022 Codenotary Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKVMetadata(t *testing.T) {
	now := time.Now()

	md := NewKVMetadata()

	bs := md.Bytes()
	require.Len(t, bs, 0)

	err := md.unsafeReadFrom(bs)
	require.NoError(t, err)
	require.False(t, md.Deleted())
	require.False(t, md.IsExpirable())
	require.False(t, md.NonIndexable())

	_, err = md.ExpirationTime()
	require.ErrorIs(t, err, ErrNonExpirable)

	require.False(t, md.ExpiredAt(now))

	t.Run("mutable methods over read-only metadata should fail", func(t *testing.T) {
		desmd := newReadOnlyKVMetadata()

		err = desmd.unsafeReadFrom(nil)
		require.NoError(t, err)

		require.False(t, desmd.Deleted())
		require.False(t, md.IsExpirable())
		require.False(t, md.ExpiredAt(now))
		require.False(t, md.NonIndexable())

		err = desmd.AsDeleted(true)
		require.ErrorIs(t, err, ErrReadOnly)

		err = desmd.ExpiresAt(now)
		require.ErrorIs(t, err, ErrReadOnly)

		err = desmd.AsNonIndexable(true)
		require.ErrorIs(t, err, ErrReadOnly)
	})

	desmd := NewKVMetadata()

	err = desmd.unsafeReadFrom(nil)
	require.NoError(t, err)

	desmd.AsDeleted(false)
	require.False(t, desmd.Deleted())

	desmd.AsDeleted(true)
	require.True(t, desmd.Deleted())

	desmd.NonExpirable()
	require.False(t, md.IsExpirable())
	require.False(t, md.ExpiredAt(now))

	desmd.ExpiresAt(now)
	require.True(t, desmd.IsExpirable())

	desmd.ExpiresAt(now)
	require.True(t, desmd.IsExpirable())

	expTime, err := desmd.ExpirationTime()
	require.NoError(t, err)
	require.Equal(t, now, expTime)
	require.True(t, desmd.ExpiredAt(now))

	desmd.AsNonIndexable(false)
	require.False(t, desmd.NonIndexable())

	desmd.AsNonIndexable(true)
	require.True(t, desmd.NonIndexable())

	bs = desmd.Bytes()
	require.NotNil(t, bs)
	require.LessOrEqual(t, len(bs), maxKVMetadataLen)

	err = desmd.unsafeReadFrom(bs)
	require.NoError(t, err)
	require.True(t, desmd.Deleted())
	require.True(t, desmd.IsExpirable())
	require.True(t, desmd.ExpiredAt(now))
	require.True(t, desmd.NonIndexable())
}
