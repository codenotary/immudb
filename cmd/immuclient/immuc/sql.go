/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
package immuc

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/olekukonko/tablewriter"
)

func (i *immuc) SQLExec(args []string) (string, error) {
	sqlStmt := strings.Join(args, " ")
	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.SQLExec(ctx, sqlStmt, nil)
	})
	if err != nil {
		return "", err
	}

	txMetas := response.(*schema.SQLExecResult)

	return fmt.Sprintf("sql ok, Ctxs: %d Dtxs: %d", len(txMetas.Ctxs), len(txMetas.Dtxs)), nil
}

func (i *immuc) SQLQuery(args []string) (string, error) {
	sqlStmt := strings.Join(args, " ")
	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		resp, err := immuClient.SQLQuery(ctx, sqlStmt, nil, true)
		if err != nil {
			return nil, err
		}
		return renderTableResult(resp), nil
	})
	if err != nil {
		return "", err
	}

	return response.(string), nil
}

func (i *immuc) ListTables() (string, error) {
	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		resp, err := immuClient.ListTables(ctx)
		if err != nil {
			return nil, err
		}
		return renderTableResult(resp), nil
	})
	if err != nil {
		return "", err
	}
	return response.(string), nil
}

func (i *immuc) DescribeTable(args []string) (string, error) {
	if len(args) != 1 {
		return "", client.ErrIllegalArguments
	}
	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		resp, err := immuClient.DescribeTable(ctx, args[0])
		if err != nil {
			return nil, err
		}
		return renderTableResult(resp), nil
	})
	if err != nil {
		return "", err
	}
	return response.(string), nil
}

func renderTableResult(resp *schema.SQLQueryResult) string {
	if resp == nil {
		return ""
	}
	result := bytes.NewBuffer([]byte{})
	consoleTable := tablewriter.NewWriter(result)
	cols := make([]string, len(resp.Columns))
	for i, c := range resp.Columns {
		cols[i] = c.Name
	}
	consoleTable.SetHeader(cols)

	for _, r := range resp.Rows {
		row := make([]string, len(r.Values))

		for i, v := range r.Values {
			row[i] = schema.RenderValue(v.Value)
		}

		consoleTable.Append(row)
	}

	consoleTable.Render()
	return result.String()
}
