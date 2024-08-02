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

package multiapp

import (
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/mocked"
	"github.com/codenotary/immudb/embedded/cache"
	"github.com/stretchr/testify/require"
)

func TestAppendableCache(t *testing.T) {
	genericCache, err := cache.NewCache(5)
	require.NoError(t, err)
	c := appendableCache{cache: genericCache}

	m1 := &mocked.MockedAppendable{}
	id, app, err := c.Put(1, m1)
	require.NoError(t, err)
	require.Nil(t, app)
	require.Zero(t, id)

	app, err = c.Get(1)
	require.NoError(t, err)
	require.Equal(t, m1, app)

	err = c.Apply(func(k int64, v appendable.Appendable) error {
		require.EqualValues(t, 1, k)
		require.Equal(t, m1, v)
		return nil
	})
	require.NoError(t, err)

	for i := 2; i < 6; i++ {
		id, app, err = c.Put(int64(i), &mocked.MockedAppendable{})
		require.NoError(t, err)
		require.Zero(t, id)
		require.Nil(t, app)
	}

	m2 := &mocked.MockedAppendable{}
	id, app, err = c.Put(7, m2)
	require.NoError(t, err)
	require.EqualValues(t, 2, id)
	require.Equal(t, m1, app)

	m3 := &mocked.MockedAppendable{}
	app, err = c.Replace(7, m3)
	require.NoError(t, err)
	require.Equal(t, m2, app)

	err = c.Apply(func(k int64, v appendable.Appendable) error {
		if k == 7 {
			require.Equal(t, m3, v)
		}
		return nil
	})
	require.NoError(t, err)

	app, err = c.Pop(7)
	require.NoError(t, err)
	require.Equal(t, m3, app)

	err = c.Apply(func(k int64, v appendable.Appendable) error {
		require.NotEqualValues(t, 7, k)
		return nil
	})
	require.NoError(t, err)
}
