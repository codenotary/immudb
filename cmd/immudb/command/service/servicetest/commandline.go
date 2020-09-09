package servicetest

import "github.com/spf13/cobra"

type commandlineMock struct{}

func (cl *commandlineMock) checkLoggedInAndConnect(cmd *cobra.Command, args []string) (err error) {
	return nil
}

func (cl *commandlineMock) disconnect(cmd *cobra.Command, args []string) {}

func (cl *commandlineMock) connect(cmd *cobra.Command, args []string) (err error) {
	return nil
}
func (cl *commandlineMock) checkLoggedIn(cmd *cobra.Command, args []string) (err error) {
	return nil
}
