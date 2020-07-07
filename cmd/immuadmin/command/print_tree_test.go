package immuadmin

import (
	"bytes"
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"io/ioutil"
	"testing"
)

func TestCommandLine_PrintTreeInit(t *testing.T) {
	immuServer = newServer(false)
	immuServer.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
	}
	opts := Options()
	opts.DialOptions = &dialOptions
	cmdl := commandline{
		context: context.Background(),
		options: opts,
	}
	_ = cmdl.connect(&cobra.Command{}, []string{})

	cmdl.printTree(&cobra.Command{})

}

func TestCommandLine_PrintTree(t *testing.T) {
	immuServer = newServer(false)
	immuServer.Start()
	immuServer.Set(context.TODO(), &schema.KeyValue{Key: []byte(`myFirstElementKey`), Value: []byte(`firstValue`)})

	cmd := cobra.Command{}
	cl := new(commandline)
	cl.context = context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
	}

	cl.options = Options()
	cl.options.DialOptions = &dialOptions
	cl.printTree(&cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"print"})
	cmd.Execute()
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Immudb Merkle Tree")

}
