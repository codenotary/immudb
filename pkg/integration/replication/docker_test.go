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
