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

package singleapp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInvalidOptions(t *testing.T) {
	for _, d := range []struct {
		n    string
		opts *Options
	}{
		{"nil", nil},
		{"empty", &Options{}},
		{"ReadBufferSize", DefaultOptions().WithReadBufferSize(0)},
		{"WriteBuffer", DefaultOptions().WithReadOnly(false).WithWriteBuffer(nil)},
	} {
		t.Run(d.n, func(t *testing.T) {
			require.ErrorIs(t, d.opts.Validate(), ErrInvalidOptions)
		})
	}
}

func TestDefaultOptions(t *testing.T) {
	require.NoError(t, DefaultOptions().Validate())
}

func TestValidOptions(t *testing.T) {
	opts := &Options{}

	require.Equal(t, DefaultFileMode, opts.WithFileMode(DefaultFileMode).fileMode)
	require.Equal(t, []byte{}, opts.WithMetadata([]byte{}).metadata)
	require.Equal(t, DefaultCompressionFormat, opts.WithCompressionFormat(DefaultCompressionFormat).compressionFormat)
	require.Equal(t, DefaultCompressionFormat, opts.WithCompressionFormat(DefaultCompressionFormat).GetCompressionFormat())
	require.Equal(t, DefaultCompressionLevel, opts.WithCompresionLevel(DefaultCompressionLevel).compressionLevel)
	require.Equal(t, DefaultCompressionLevel, opts.WithCompresionLevel(DefaultCompressionLevel).GetCompressionLevel())

	require.True(t, opts.WithRetryableSync(true).retryableSync)
	require.True(t, opts.WithAutoSync(true).autoSync)

	require.True(t, opts.WithReadOnly(true).readOnly)
	require.ErrorIs(t, opts.Validate(), ErrInvalidOptions)

	require.Equal(t, DefaultReadBufferSize+1, opts.WithReadBufferSize(DefaultReadBufferSize+1).GetReadBufferSize())
	require.NoError(t, opts.Validate())

	opts.WithPreallocSize(-1)
	require.ErrorIs(t, opts.Validate(), ErrInvalidOptions)

	require.Equal(t, 10, opts.WithPreallocSize(10).GetPreallocSize())
	require.NoError(t, opts.Validate())

	require.False(t, opts.WithReadOnly(false).readOnly)
	require.ErrorIs(t, opts.Validate(), ErrInvalidOptions)

	b := make([]byte, DefaultWriteBufferSize)
	require.Equal(t, b, opts.WithWriteBuffer(b).GetWriteBuffer())
	require.NoError(t, opts.Validate())

	require.True(t, opts.WithReadOnly(true).readOnly)
	require.NoError(t, opts.Validate())
}
