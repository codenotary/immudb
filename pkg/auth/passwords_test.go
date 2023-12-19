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

package auth

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsStrongPassword(t *testing.T) {
	weakPass := "pass"
	if err := IsStrongPassword(weakPass); err == nil {
		t.Errorf("IsStrongPassword failed to detect week password")
	}
	weakPass = "1~password"
	if err := IsStrongPassword(weakPass); err == nil {
		t.Errorf("IsStrongPassword failed to detect week password")
	}
	weakPass = "1~Password"
	if err := IsStrongPassword(weakPass); err != nil {
		t.Errorf("IsStrongPassword detected wrong week password")
	}
	weakPass = "1~Password\n"
	if err := IsStrongPassword(weakPass); err == nil {
		t.Errorf("IsStrongPassword failed to detect non allowed character")
	}
}

func TestDecodeBase64Password(t *testing.T) {
	pass := "pass"
	_, err := DecodeBase64Password(pass)
	require.NoError(t, err)

	pass = "enc:" + base64.StdEncoding.EncodeToString([]byte("password"))
	decodedPass, err := DecodeBase64Password(pass)
	require.NoError(t, err)
	require.Equal(t, "password", decodedPass)

	_, err = DecodeBase64Password(strings.TrimSuffix(pass, "="))
	require.ErrorContains(t, err, "error decoding password from base64 string")
}
