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

	md := KVMetadata{}

	bs := md.Bytes()
	require.NotNil(t, bs)
	require.Len(t, bs, 1)
	require.Equal(t, byte(0), bs[0])

	err := md.ReadFrom(bs)
	require.NoError(t, err)
	require.False(t, md.Deleted())
	require.Nil(t, md.expiresAt)
	require.False(t, md.Expirable())
	require.Nil(t, md.ExpirationTime())
	require.False(t, md.ExpiredAt(now))

	desmd := KVMetadata{}
	err = desmd.ReadFrom(nil)
	require.NoError(t, err)
	require.False(t, desmd.Deleted())
	require.False(t, md.Expirable())
	require.False(t, md.ExpiredAt(now))

	desmd.AsDeleted(true)
	require.True(t, desmd.Deleted())

	desmd.ExpiresAt(nil)
	require.False(t, md.Expirable())
	require.False(t, md.ExpiredAt(now))

	desmd.ExpiresAt(&now)
	require.True(t, desmd.Expirable())
	require.Equal(t, &now, desmd.ExpirationTime())
	require.True(t, desmd.ExpiredAt(now))

	bs = desmd.Bytes()
	require.NotNil(t, bs)
	require.Len(t, bs, 1+tsSize)
	require.NotEqual(t, byte(0), bs[0])

	err = desmd.ReadFrom(bs)
	require.NoError(t, err)
	require.True(t, desmd.Deleted())
	require.True(t, desmd.Expirable())
	require.True(t, desmd.ExpiredAt(now))
}
