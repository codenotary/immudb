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
package immuc

import (
	"context"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) SQLExec(args []string) (CommandOutput, error) {
	sqlStmt := strings.Join(args, " ")
	ctx := context.Background()
	response, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.SQLExec(ctx, sqlStmt, nil)
	})
	if err != nil {
		return nil, err
	}

	sqlRes := response.(*schema.SQLExecResult)

	var updatedRows int

	for _, tx := range sqlRes.Txs {
		updatedRows += int(tx.UpdatedRows)
	}

	return &sqlExecOutput{
		UpdatedRows: updatedRows,
	}, nil
}

func (i *immuc) SQLQuery(args []string) (CommandOutput, error) {
	sqlStmt := strings.Join(args, " ")
	ctx := context.Background()
	response, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		resp, err := immuClient.SQLQuery(ctx, sqlStmt, nil, true)
		if err != nil {
			return nil, err
		}
		return resp, nil
	})
	if err != nil {
		return nil, err
	}

	return &tableOutput{
		resp: response.(*schema.SQLQueryResult),
	}, nil
}

func (i *immuc) ListTables() (CommandOutput, error) {
	ctx := context.Background()
	response, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		resp, err := immuClient.ListTables(ctx)
		if err != nil {
			return nil, err
		}
		return resp, nil
	})
	if err != nil {
		return nil, err
	}
	return &tableOutput{
		resp: response.(*schema.SQLQueryResult),
	}, nil
}

func (i *immuc) DescribeTable(args []string) (CommandOutput, error) {
	if len(args) != 1 {
		return nil, client.ErrIllegalArguments
	}
	ctx := context.Background()
	response, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		resp, err := immuClient.DescribeTable(ctx, args[0])
		if err != nil {
			return nil, err
		}
		return resp, nil
	})
	if err != nil {
		return nil, err
	}
	return &tableOutput{
		resp: response.(*schema.SQLQueryResult),
	}, nil
}
