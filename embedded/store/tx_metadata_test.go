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

	"github.com/stretchr/testify/require"
)

func TestTxMetadata(t *testing.T) {
	md := NewTxMetadata()

	require.True(t, md.IsEmpty())

	bs := md.Bytes()
	require.Nil(t, bs)

	err := md.ReadFrom(bs)
	require.NoError(t, err)

	desmd := NewTxMetadata()
	err = desmd.ReadFrom(nil)
	require.NoError(t, err)

	err = desmd.ReadFrom(desmd.Bytes())
	require.NoError(t, err)

	require.True(t, md.Equal(desmd))
	require.True(t, desmd.IsEmpty())
}

func TestTxMetadataWithAttributes(t *testing.T) {
	md := NewTxMetadata()

	bs := md.Bytes()
	require.Len(t, bs, 0)

	err := md.ReadFrom(bs)
	require.NoError(t, err)
	require.False(t, md.HasTruncatedTxID())

	desmd := NewTxMetadata()

	err = desmd.ReadFrom(nil)
	require.NoError(t, err)

	_, err = desmd.GetTruncatedTxID()
	require.ErrorIs(t, err, ErrTruncationInfoNotPresentInMetadata)

	desmd.WithTruncatedTxID(1)
	require.True(t, desmd.HasTruncatedTxID())

	v, err := desmd.GetTruncatedTxID()
	require.NoError(t, err)
	require.Equal(t, uint64(1), v)

	desmd.WithTruncatedTxID(10)
	v, err = desmd.GetTruncatedTxID()
	require.NoError(t, err)
	require.Equal(t, uint64(10), v)

	require.False(t, desmd.IsEmpty())

	changes := desmd.GetIndexingChanges()
	require.Nil(t, changes)

	specChanges := make(map[int]IndexChange)
	desmd.WithIndexingChanges(specChanges)
	changes = desmd.GetIndexingChanges()
	require.Nil(t, changes)

	specChanges[1] = &IndexDeletionChange{}
	specChanges[2] = &IndexCreationChange{}

	desmd.WithIndexingChanges(specChanges)
	changes = desmd.GetIndexingChanges()
	require.Len(t, changes, 2)

	bs = desmd.Bytes()
	require.NotNil(t, bs)
	require.Less(t, len(bs), maxTxMetadataLen)

	err = desmd.ReadFrom(bs)
	require.NoError(t, err)
	require.True(t, desmd.HasTruncatedTxID())
	require.Len(t, desmd.GetIndexingChanges(), 2)
}
