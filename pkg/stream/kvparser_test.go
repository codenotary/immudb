/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package stream

import (
	"bytes"
	"errors"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestParseKV(t *testing.T) {
	content := []byte(`contentval`)
	value, err := ReadValue(bytes.NewBuffer(content), 4096)
	require.NoError(t, err)
	require.NotNil(t, value)
}

func TestParseErr(t *testing.T) {
	b := &streamtest.ErrReader{ReadF: func(i []byte) (int, error) {
		return 0, errors.New("custom one")
	}}
	entry, err := ReadValue(b, 4096)
	require.Error(t, err)
	require.Nil(t, entry)
}

func TestParseEof(t *testing.T) {
	b := &streamtest.ErrReader{ReadF: func(i []byte) (int, error) {
		return 0, io.EOF
	}}
	entry, err := ReadValue(b, 4096)
	require.Equal(t, io.EOF, err)
	require.Nil(t, entry)
}

func TestParseEmptyContent(t *testing.T) {
	content := []byte{}
	value, err := ReadValue(bytes.NewBuffer(content), 4096)
	require.Equal(t, io.EOF, err)
	require.Nil(t, value)
}
