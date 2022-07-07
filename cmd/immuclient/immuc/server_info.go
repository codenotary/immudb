package immuc

import (
	"context"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) ServerInfo(args []string) (string, error) {
	ctx := context.Background()
	resp, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.ServerInfo(ctx, &schema.ServerInfoRequest{})
	})
	if err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}
	return PrintServerInfo(resp.(*schema.ServerInfoResponse)), nil
}
