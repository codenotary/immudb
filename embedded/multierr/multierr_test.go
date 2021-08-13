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
package multierr

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiErr(t *testing.T) {
	includedErrors := []error{
		errors.New("error1"),
		errors.New("error2"),
	}

	excludedErr := errors.New("error3")

	merr := NewMultiErr()
	require.NotNil(t, merr)
	require.False(t, merr.HasErrors())
	require.Empty(t, merr.Errors())

	merr.Append(includedErrors[0]).Append(includedErrors[1])

	require.Error(t, merr)
	require.True(t, merr.HasErrors())
	require.Len(t, merr.Errors(), 2)
	require.True(t, merr.Includes(includedErrors[0]))
	require.True(t, merr.Includes(includedErrors[1]))
	require.False(t, merr.Includes(excludedErr))

	require.Contains(t, merr.Error(), "error1")
	require.Contains(t, merr.Error(), "error2")
	require.NotContains(t, merr.Error(), "error3")
}
