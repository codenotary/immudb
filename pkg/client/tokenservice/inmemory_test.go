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

package tokenservice

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewInmemoryTokenService(t *testing.T) {
	ts := NewInmemoryTokenService()
	err := ts.SetToken("db", "tk")
	require.NoError(t, err)
	_, err = ts.IsTokenPresent()
	require.NoError(t, err)
	_, err = ts.GetDatabase()
	require.NoError(t, err)
	_, err = ts.GetToken()
	require.NoError(t, err)
	err = ts.DeleteToken()
	require.NoError(t, err)
}
