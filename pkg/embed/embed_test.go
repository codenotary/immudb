package embed

import (
	"fmt"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestImmudb_SimpleReadWrite(t *testing.T) {
	db, err := Open()
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(filepath.Join(DefaultDir, DefaultDatabseName))
	}()

	err = db.VerifiedSet([]byte("key"), []byte("value"))
	require.NoError(t, err)
	ventry, err := db.VerifiedGet([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("key"), ventry.Key)
	require.Equal(t, []byte("value"), ventry.Value)
}

func TestImmudb_ParallelReadWrite(t *testing.T) {
	db, err := Open()
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(filepath.Join(DefaultDir, DefaultDatabseName))
	}()

	for i := 0; i < 10; i++ {
		go func(i int) {
			err = db.VerifiedSet([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
			require.NoError(t, err)
			ventry, err := db.VerifiedGet([]byte(fmt.Sprintf("%d", i)))
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("%d", i)), ventry.Key)
			require.Equal(t, []byte(fmt.Sprintf("%d", i)), ventry.Value)
		}(i)
	}
}

func TestImmudb_Immudb_StateServiceReadWrite(t *testing.T) {
	options := server.DefaultOptions()
	bs := servertest.NewBufconnServer(options)

	bs.Start()

	opts := client.DefaultOptions().
		WithUsername("immudb").
		WithPassword("immudb").
		WithDatabase("defaultdb")

	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	immudbSS, err := NewEmbedStateService(opts)
	require.NoError(t, err)
	db, err := Open(StateService(immudbSS))
	require.NoError(t, err)
	defer func() {
		bs.Stop()
		os.Remove(".state-" + bs.Server.Srv.UUID.String())
		os.RemoveAll(options.Dir)
		os.Remove(filepath.Join(DefaultDir, "immudb.identifier"))
		os.RemoveAll(filepath.Join(DefaultDir, DefaultDatabseName))
	}()

	err = db.VerifiedSet([]byte("key"), []byte("value"))
	require.NoError(t, err)
	ventry, err := db.VerifiedGet([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("key"), ventry.Key)
	require.Equal(t, []byte("value"), ventry.Value)

	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err = db.VerifiedSet([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
			require.NoError(t, err)
			ventry, err := db.VerifiedGet([]byte(fmt.Sprintf("%d", i)))
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("%d", i)), ventry.Key)
			require.Equal(t, []byte(fmt.Sprintf("%d", i)), ventry.Value)
		}(i)
	}
	wg.Wait()
}
