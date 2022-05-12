/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"
	"net/http"
)

func Launch(cmd *cobra.Command) error {
	immuPort, err := cmd.Flags().GetInt("immudb-port")
	if err != nil {
		return err
	}
	immuAddress, err := cmd.Flags().GetString("immudb-address")
	if err != nil {
		return err
	}
	cli, err := client.NewImmuClient(client.DefaultOptions().WithPort(immuPort).WithAddress(immuAddress))
	if err != nil {
		return err
	}

	ctx, err := getAuthContext(cli)
	if err != nil {
		return err
	}
	sf, err := cmd.Flags().GetString("source-file")
	if err != nil {
		return err
	}
	h := &handler{
		cli: cli,
		ctx: ctx,
		sf:  sf,
	}

	http.HandleFunc("/upload", h.upload)
	http.HandleFunc("/stream", h.stream)

	port, err := cmd.Flags().GetInt("port")
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Serving on port %d\n", port)
	err = http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		return err
	}
	return nil
}

func getAuthContext(cli client.ImmuClient) (context.Context, error) {
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		return nil, err
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{Databasename: "defaultdb"})
	if err != nil {
		return nil, err
	}

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	return ctx, nil
}
