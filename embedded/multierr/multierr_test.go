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

package multierr

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type includedErrA struct {
	err string
}

func (e *includedErrA) Error() string {
	return e.err
}

type includedErrB struct {
	err string
}

func (e *includedErrB) Error() string {
	return e.err
}

type excludedErr struct {
	err string
}

func (e *excludedErr) Error() string {
	return e.err
}

func TestMultiErr(t *testing.T) {
	includedErrors := []error{
		&includedErrA{err: "includedErrorA1"},
		&includedErrA{err: "includedErrorA2"},
		&includedErrB{err: "includedErrorB1"},
	}

	eErr := &excludedErr{err: "excludedError1"}

	merr := NewMultiErr()
	require.NotNil(t, merr)
	require.False(t, merr.HasErrors())
	require.Empty(t, merr.Errors())
	require.Nil(t, merr.Reduce())

	merr.Append(includedErrors[0]).
		Append(includedErrors[1]).
		Append(includedErrors[2])

	require.Error(t, merr)
	require.True(t, merr.HasErrors())
	require.Len(t, merr.Errors(), 3)
	require.True(t, merr.Includes(includedErrors[0]))
	require.True(t, merr.Includes(includedErrors[1]))
	require.True(t, merr.Includes(includedErrors[2]))
	require.False(t, merr.Includes(eErr))

	require.ErrorIs(t, merr, includedErrors[0])
	require.ErrorIs(t, merr, includedErrors[1])
	require.NotErrorIs(t, merr, eErr)

	require.Contains(t, merr.Error(), "includedErrorA1")
	require.Contains(t, merr.Error(), "includedErrorA2")
	require.Contains(t, merr.Error(), "includedErrorB1")
	require.NotContains(t, merr.Error(), "excludedError1")

	require.Equal(t, merr, merr.Reduce())

	var iErrA *includedErrA
	require.ErrorAs(t, merr, &iErrA)
	require.NotNil(t, iErrA)

	var iErrB *includedErrB
	require.ErrorAs(t, merr, &iErrB)
	require.NotNil(t, iErrB)

	var eErr2 *excludedErr
	require.False(t, errors.As(merr, &eErr2))
	require.Nil(t, eErr2)
}
