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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTxMetadata(t *testing.T) {
	md := &TxMetadata{}

	bs := md.Bytes()
	require.NotNil(t, bs)
	require.Len(t, bs, 2)
	require.Equal(t, uint16(0), binary.BigEndian.Uint16(bs))

	err := md.ReadFrom(bs)
	require.NoError(t, err)
	require.Empty(t, md.Summary())

	desmd := &TxMetadata{}
	err = desmd.ReadFrom(nil)
	require.NoError(t, err)
	require.Nil(t, desmd.Summary())

	desmd.WithSummary([]byte{0, 1, 2, 3})
	require.Equal(t, []byte{0, 1, 2, 3}, desmd.Summary())

	err = desmd.ReadFrom(desmd.Bytes())
	require.NoError(t, err)
	require.Equal(t, []byte{0, 1, 2, 3}, desmd.Summary())

	require.False(t, md.Equal(nil))
	require.False(t, md.Equal(desmd))

	md.WithSummary([]byte{0, 1, 2, 3})
	require.True(t, md.Equal(desmd))
}
