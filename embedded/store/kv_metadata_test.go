/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

	err := md.ReadFrom(bs)
	require.NoError(t, err)
	require.False(t, md.Deleted())
	require.False(t, md.IsExpirable())

	_, err = md.ExpirationTime()
	require.ErrorIs(t, err, ErrNonExpirable)

	require.False(t, md.ExpiredAt(now))

	desmd := NewKVMetadata()
	err = desmd.ReadFrom(nil)
	require.NoError(t, err)
	require.False(t, desmd.Deleted())
	require.False(t, md.IsExpirable())
	require.False(t, md.ExpiredAt(now))

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

	bs = desmd.Bytes()
	require.NotNil(t, bs)
	require.Len(t, bs, maxKVMetadataLen)

	err = desmd.ReadFrom(bs)
	require.NoError(t, err)
	require.True(t, desmd.Deleted())
	require.True(t, desmd.IsExpirable())
	require.True(t, desmd.ExpiredAt(now))
}
