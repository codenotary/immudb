package commands

import (
	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/spf13/cobra"
)

func (cl *commandline) getImmuClient(cmd *cobra.Command) *client.ImmuClient {
	options, err := cl.options(cmd)
	if err != nil {
		c.QuitToStdErr(err)
	}
	dt, err := timestamp.NewTdefault()
	if err != nil {
		c.QuitToStdErr(err)
	}
	ts := client.NewTimestampService(dt)
	immuClient := client.
		DefaultClient().
		WithOptions(*options).
		WithTimestampService(ts)
	return immuClient
}
