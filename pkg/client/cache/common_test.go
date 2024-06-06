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

package cache

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateServerIdentityInFile(t *testing.T) {
	identityDir := t.TempDir()

	t.Run("creating new identity files must not fail", func(t *testing.T) {
		err := validateServerIdentityInFile("identity1", "uuid1", identityDir)
		require.NoError(t, err)

		err = validateServerIdentityInFile("identity2", "uuid2", identityDir)
		require.NoError(t, err)
	})

	t.Run("validating server identity for already known server must not fail", func(t *testing.T) {
		err := validateServerIdentityInFile("identity1", "uuid1", identityDir)
		require.NoError(t, err)

		err = validateServerIdentityInFile("identity2", "uuid2", identityDir)
		require.NoError(t, err)
	})

	t.Run("validating server identity for wrong server must fail", func(t *testing.T) {
		err := validateServerIdentityInFile("identity1", "uuid2", identityDir)
		require.ErrorIs(t, err, ErrServerIdentityValidationFailed)

		err = validateServerIdentityInFile("identity2", "uuid1", identityDir)
		require.ErrorIs(t, err, ErrServerIdentityValidationFailed)
	})

}

func TestValidateServerIdentityInFileCornerCases(t *testing.T) {
	t.Run("fail to validate identity file with invalid path", func(t *testing.T) {
		err := validateServerIdentityInFile("identity1", "uuid1", "/invalid/folder/name?")
		require.ErrorContains(t, err, "could not check the identity of the server")
		// This is not validation error, it's some OS error
		require.NotErrorIs(t, err, ErrServerIdentityValidationFailed)
	})

	t.Run("fail to validate identity file with broken content", func(t *testing.T) {
		identityDir := t.TempDir()

		err := ioutil.WriteFile(
			getFilenameForServerIdentity("identity1", identityDir),
			[]byte("this is not a json content!!!"),
			0644,
		)
		require.NoError(t, err)

		err = validateServerIdentityInFile("identity1", "uuid1", identityDir)
		var jsonError *json.SyntaxError
		require.ErrorAs(t, err, &jsonError)
		require.NotErrorIs(t, err, ErrServerIdentityValidationFailed)
	})
}
