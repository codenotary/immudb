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
package chbuffer

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCHBuffer(t *testing.T) {
	size := 256
	b := New(size)

	_, err := b.Get()
	require.Error(t, ErrBufferIsEmpty, err)

	for i := 0; i < size; i++ {
		err := b.Put(sha256.Sum256([]byte{byte(i)}))
		require.NoError(t, err)
	}

	err = b.Put(sha256.Sum256(nil))
	require.Error(t, ErrBufferIsFull, err)

	for i := 0; i < size; i++ {
		h, err := b.Get()
		require.NoError(t, err)
		require.Equal(t, sha256.Sum256([]byte{byte(i)}), h)
	}

	_, err = b.Get()
	require.Error(t, ErrBufferIsEmpty, err)
}
