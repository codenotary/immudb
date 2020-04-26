package commands

import (
	"io/ioutil"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func (cl *commandline) options(cmd *cobra.Command) (*client.Options, error) {
	port := viper.GetInt("default.port")
	address := viper.GetString("default.address")
	authEnabled := viper.GetBool("default.auth")
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithAuth(authEnabled).
		WithDialOptions(false, grpc.WithInsecure())
	if authEnabled {
		tokenBytes, err := ioutil.ReadFile(*cl.tokenFilename)
		if err == nil {
			token := string(tokenBytes)
			options = options.WithDialOptions(
				false,
				grpc.WithUnaryInterceptor(auth.ClientUnaryInterceptor(token)),
				grpc.WithStreamInterceptor(auth.ClientStreamInterceptor(token)),
			)
		}
	}

	return &options, nil
}
