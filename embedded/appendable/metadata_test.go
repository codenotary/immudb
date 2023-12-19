/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package appendable

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockedIOReader struct {
}

func (w *mockedIOReader) Read(b []byte) (int, error) {
	return 0, errors.New("error")
}

type mockedIOWriter struct {
}

func (w *mockedIOWriter) Write(b []byte) (int, error) {
	return 0, errors.New("error")
}

func TestMedatada(t *testing.T) {
	md := NewMetadata(nil)

	_, found := md.Get("intKey")
	require.False(t, found)

	_, found = md.GetInt("intKey")
	require.False(t, found)

	_, found = md.Get("boolKey")
	require.False(t, found)

	_, found = md.GetBool("boolKey")
	require.False(t, found)

	for i := 0; i < 10; i++ {
		md.PutInt(fmt.Sprintf("intKey_%d", i), i)
		md.PutBool(fmt.Sprintf("boolKey_%d", i), i%2 == 0)
	}

	for i := 0; i < 10; i++ {
		iv, found := md.GetInt(fmt.Sprintf("intKey_%d", i))
		require.True(t, found)
		require.Equal(t, i, iv)

		bv, found := md.GetBool(fmt.Sprintf("boolKey_%d", i))
		require.True(t, found)
		require.Equal(t, i%2 == 0, bv)
	}

	md1 := NewMetadata(md.Bytes())

	for i := 0; i < 10; i++ {
		v, found := md1.GetInt(fmt.Sprintf("intKey_%d", i))
		require.True(t, found)
		require.Equal(t, i, v)

		bv, found := md.GetBool(fmt.Sprintf("boolKey_%d", i))
		require.True(t, found)
		require.Equal(t, i%2 == 0, bv)
	}

	mockedReader := &mockedIOReader{}
	_, err := md.ReadFrom(mockedReader)
	require.Error(t, err)

	mockedWriter := &mockedIOWriter{}
	_, err = md.WriteTo(mockedWriter)
	require.Error(t, err)
}
