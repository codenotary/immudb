package client

import (
	"context"
	"log"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestDatabasesSwitching(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	ctx := context.Background()

	pr := &PasswordReader{
		Pass: []string{"immudb"},
	}

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(NewHomedirService())
	cliopt := DefaultOptions().WithDialOptions(&dialOptions).WithPasswordReader(pr).WithTokenService(ts)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = &dialOptions

	client, _ = NewImmuClient(cliopt)
	lresp, err := client.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}

	md := metadata.Pairs("authorization", lresp.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	err = client.CreateDatabase(ctx, &schema.Database{
		Databasename: "db1",
	})
	require.Nil(t, err)
	resp, err := client.UseDatabase(ctx, &schema.Database{
		Databasename: "db1",
	})

	md = metadata.Pairs("authorization", resp.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	assert.Nil(t, err)
	assert.NotEmpty(t, resp.Token)
	i, err := client.SafeSet(ctx, []byte(`db1-my`), []byte(`item`))
	assert.Nil(t, err)
	assert.True(t, i.Verified)

	err = client.CreateDatabase(ctx, &schema.Database{
		Databasename: "db2",
	})
	assert.Nil(t, err)
	resp2, err := client.UseDatabase(ctx, &schema.Database{
		Databasename: "db2",
	})
	md = metadata.Pairs("authorization", resp2.Token)

	ctx = metadata.NewOutgoingContext(context.Background(), md)

	assert.Nil(t, err)
	assert.NotEmpty(t, resp.Token)
	i, err = client.SafeSet(ctx, []byte(`db2-my`), []byte(`item`))
	assert.Nil(t, err)
	assert.True(t, i.Verified)
	vi, err := client.SafeGet(ctx, []byte(`db1-my`))
	assert.Error(t, err)
	assert.Nil(t, vi)
}

type PasswordReader struct {
	Pass       []string
	callNumber int
}

func (pr *PasswordReader) Read(msg string) ([]byte, error) {
	if len(pr.Pass) <= pr.callNumber {
		log.Fatal("Application requested the password more times than number of passwords supplied")
	}
	pass := []byte(pr.Pass[pr.callNumber])
	pr.callNumber++
	return pass, nil
}
