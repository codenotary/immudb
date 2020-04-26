package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
)

func (cl *commandline) login(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "login username \"password\"",
		Short:   fmt.Sprintf("Login using the specified username and \"password\" (username is \"%s\")", auth.AdminUser.Username),
		Aliases: []string{"l"},
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := cl.options(cmd)
			if err != nil {
				c.QuitToStdErr(err)
			}
			immuClient := client.
				DefaultClient().
				WithOptions(*options)
			user := []byte(args[0])
			pass := []byte(args[1])
			ctx := context.Background()
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.Login(ctx, user, pass)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			if err := ioutil.WriteFile(*cl.tokenFilename, response.(*schema.LoginResponse).Token, 0644); err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Printf("logged in\n")
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) logout(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "logout",
		Aliases: []string{"x"},
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = os.Remove(*cl.tokenFilename)
			fmt.Println("logged out")
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(ccmd)
}
