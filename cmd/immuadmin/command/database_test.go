package immuadmin

import (
	"bytes"
	"context"
	"github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"io/ioutil"
	"strings"
	"testing"
)

func TestDatabaseList(t *testing.T) {
	bs := servertest.NewBufconnServer(server.DefaultOptions().WithAuth(true).WithInMemoryStore(true))
	bs.Start()

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb"},
	}
	hds := &immuclienttest.HomedirServiceMock{}
	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(hds)
	cliopt := Options().WithDialOptions(&dialOptions).WithPasswordReader(pr).WithTokenService(ts)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = &dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}
	if err = ts.SetToken("", token.Token); err != nil {
		t.Fatal(err)
	}

	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
		ts:             ts,
	}

	cmd := cobra.Command{}

	cmdl.database(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"database", "list"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "defaultdb") {
		t.Fatal(err)
	}
}

func TestDatabaseCreate(t *testing.T) {
	bs := servertest.NewBufconnServer(server.DefaultOptions().WithAuth(true).WithInMemoryStore(true))
	bs.Start()

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb"},
	}
	hds := &immuclienttest.HomedirServiceMock{}
	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(hds)

	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options().WithDialOptions(&dialOptions).WithPasswordReader(pr).WithTokenService(ts)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = &dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}
	if err = ts.SetToken("", token.Token); err != nil {
		t.Fatal(err)
	}

	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
		ts:             ts,
	}

	cmd := cobra.Command{}

	cmdl.database(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"database", "create", "mynewdb"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "database successfully created") {
		t.Fatal(string(msg))
	}
}
