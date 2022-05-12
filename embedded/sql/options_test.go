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
package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptions(t *testing.T) {
	opts := &Options{}

	require.False(t, ValidOpts(nil))
	require.False(t, ValidOpts(opts))

	opts.WithDistinctLimit(0)
	require.False(t, ValidOpts(opts))

	opts.WithDistinctLimit(defultDistinctLimit)
	require.Equal(t, defultDistinctLimit, opts.distinctLimit)

	opts.WithPrefix([]byte("sqlPrefix"))
	require.Equal(t, []byte("sqlPrefix"), opts.prefix)

	opts.WithAutocommit(true)
	require.True(t, opts.autocommit)

	require.True(t, ValidOpts(opts))
}
