package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	c "github.com/codenotary/immudb/cmd"
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
			user := []byte(args[0])
			pass := []byte(args[1])
			ctx := context.Background()
			response, err := cl.immuClient.Login(ctx, user, pass)
			if err != nil {
				c.QuitWithUserError(err)
			}
			if err := ioutil.WriteFile(client.TokenFileName, response.Token, 0644); err != nil {
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
			if err := os.Remove(client.TokenFileName); err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Println("logged out")
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(ccmd)
}
