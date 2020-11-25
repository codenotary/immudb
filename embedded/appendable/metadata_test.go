/*
Copyright 2019-2020 vChain, Inc.

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

	_, found := md.Get("key")
	require.False(t, found)

	_, found = md.GetInt("key")
	require.False(t, found)

	for i := 0; i < 10; i++ {
		md.PutInt(fmt.Sprintf("key_%d", i), i)
	}

	for i := 0; i < 10; i++ {
		v, found := md.GetInt(fmt.Sprintf("key_%d", i))
		require.True(t, found)
		require.Equal(t, i, v)
	}

	md1 := NewMetadata(md.Bytes())

	for i := 0; i < 10; i++ {
		v, found := md1.GetInt(fmt.Sprintf("key_%d", i))
		require.True(t, found)
		require.Equal(t, i, v)
	}

	mockedReader := &mockedIOReader{}
	err := md.ReadFrom(mockedReader)
	require.Error(t, err)

	mockedWriter := &mockedIOWriter{}
	_, err = md.WriteTo(mockedWriter)
	require.Error(t, err)
}
