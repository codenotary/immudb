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

package ring

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRingBuffer(t *testing.T) {
	rb := NewRingBuffer(10)
	var def uint64 = 0
	require.Equal(t, def, rb.Head())
	require.Equal(t, def, rb.Tail())
	require.Equal(t, nil, rb.Get(1))
	require.Equal(t, nil, rb.Get(11))
	for i := 0; i < 10; i++ {
		rb.Set(uint64(i), i+5)
	}
	rb.Set(11, 11+5)
	require.Equal(t, uint64(1), rb.Head())
	require.Equal(t, nil, rb.Get(0))
	require.Equal(t, uint64(12), rb.Tail())
	rb.Set(12, 12+5)
	require.Equal(t, nil, rb.Get(2))
	require.Equal(t, 9, rb.Get(4))
	require.Equal(t, uint64(13), rb.Tail())
}
