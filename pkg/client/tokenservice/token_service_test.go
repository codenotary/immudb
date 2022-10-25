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

package tokenservice

import (
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/client/homedir"

	"github.com/stretchr/testify/require"
)

func TestTokenSevice_setToken(t *testing.T) {
	fn := "deleteme"
	ts := file{tokenFileName: fn, hds: homedir.NewHomedirService()}
	err := ts.SetToken("db1", "")
	require.Equal(t, ErrEmptyTokenProvided, err)
	err = ts.SetToken("db1", "toooooken")
	require.NoError(t, err)
	database, err := ts.GetDatabase()
	require.NoError(t, err)
	require.Equal(t, "db1", database)
	token, err := ts.GetToken()
	require.NoError(t, err)
	require.Equal(t, "toooooken", token)
	os.Remove(fn)
}

func TestTokenService_IsTokenPresent(t *testing.T) {
	fn := "deleteme"
	ts := file{tokenFileName: fn, hds: homedir.NewHomedirService()}
	err := ts.SetToken("db1", "toooooken")
	require.NoError(t, err)
	ok, err := ts.IsTokenPresent()
	require.NoError(t, err)
	require.True(t, ok)
}

func TestTokenService_DeleteToken(t *testing.T) {
	fn := "deleteme"
	ts := file{tokenFileName: fn, hds: homedir.NewHomedirService()}
	err := ts.SetToken("db1", "toooooken")
	require.NoError(t, err)
	err = ts.DeleteToken()
	require.NoError(t, err)
}
