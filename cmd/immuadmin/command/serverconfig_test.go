package immuadmin

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestCommandLine_ServerconfigAuth(t *testing.T) {
	input, _ := ioutil.ReadFile("../../../test/immudb.toml")
	err := ioutil.WriteFile("/tmp/immudb.toml", input, 0644)
	if err != nil {
		panic(err)
	}

	options := server.Options{}.WithAuth(false).WithInMemoryStore(true).WithConfig("/tmp/immudb.toml")
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options()
	cliopt.DialOptions = &dialOptions

	cl := commandline{
		options:        cliopt,
		immuClient:     &scIClientMock{*new(client.ImmuClient)},
		passwordReader: pwReaderMock,
		context:        context.Background(),
		hds:            newHomedirServiceMock(),
	}

	cmdso := &cobra.Command{}
	cl.serverConfig(cmdso)

	b := bytes.NewBufferString("")
	cmdso.SetOut(b)
	cmdso.SetArgs([]string{"set", "auth", "password"})
	cmdso.Execute()
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Server auth config updated")
	os.RemoveAll("/tmp/immudb.toml")
}

func TestCommandLine_ServerconfigMtls(t *testing.T) {
	input, _ := ioutil.ReadFile("../../../test/immudb.toml")
	err := ioutil.WriteFile("/tmp/immudb.toml", input, 0644)
	if err != nil {
		panic(err)
	}
	options := server.Options{}.WithAuth(false).WithInMemoryStore(true).WithConfig("/tmp/immudb.toml")
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options()
	cliopt.DialOptions = &dialOptions

	cl := commandline{
		options:        cliopt,
		immuClient:     &scIClientMock{*new(client.ImmuClient)},
		passwordReader: pwReaderMock,
		context:        context.Background(),
		hds:            newHomedirServiceMock(),
	}

	cmdso := &cobra.Command{}
	cl.serverConfig(cmdso)

	b := bytes.NewBufferString("")
	cmdso.SetOut(b)
	cmdso.SetArgs([]string{"set", "mtls", "false"})
	cmdso.Execute()
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Server MTLS config updated")

	os.RemoveAll("/tmp/immudb.toml")
}

type scIClientMock struct {
	client.ImmuClient
}

func (c scIClientMock) UpdateAuthConfig(ctx context.Context, kind auth.Kind) error {
	return nil
}
func (c scIClientMock) UpdateMTLSConfig(ctx context.Context, enabled bool) error {
	return nil
}
func (c scIClientMock) Disconnect() error {
	return nil
}
func (c scIClientMock) Logout(ctx context.Context) error {
	return nil
}

func (c scIClientMock) GetOptions() *client.Options {
	dialOptions := []grpc.DialOption{}
	return &client.Options{
		DialOptions: &dialOptions,
	}
}

func (c scIClientMock) Login(ctx context.Context, user []byte, pass []byte) (*schema.LoginResponse, error) {
	return &schema.LoginResponse{}, nil
}
