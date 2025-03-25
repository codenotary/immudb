/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package replication

// var pool *dockertest.Pool

// func TestMain(m *testing.M) {
// 	var err error
// 	pool, err = dockertest.NewPool("")
// 	if err != nil {
// 		log.Fatalf("Could not connect to docker: %s", err)
// 	}
// 	os.Exit(m.Run())
// }

// func TestImmudb(t *testing.T) {
// 	resource, err := pool.Run("codenotary/immudb", "latest", []string{})
// 	require.NoError(t, err)

// 	assert.NotEmpty(t, resource.GetPort("3322/tcp"))
// 	assert.NotEmpty(t, resource.GetBoundIP("3322/tcp"))

// 	require.Nil(t, pool.Purge(resource))
// }

// func TestDockerTestServer(t *testing.T) {
// 	d := &dockerTestServer{
// 		pool: pool,
// 	}
// 	d.Start(t)

// 	addr, port := d.Address(t)
// 	assert.NotEmpty(t, addr)
// 	assert.NotEmpty(t, port)

// 	d.Shutdown(t)
// }
