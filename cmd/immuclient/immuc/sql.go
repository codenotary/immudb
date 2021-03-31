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
	"io/ioutil"
	"math"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) SQLExec(args []string) (string, error) {
	sqlStmt, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.SQLExec(ctx, &schema.SQLExecRequest{Sql: string(sqlStmt)})
	})
	if err != nil {
		return "", err
	}

	txMetas := response.(*schema.SQLExecResult)

	return fmt.Sprintf("sql ok, Ctxs: %d Dtxs: %d", len(txMetas.Ctxs), len(txMetas.Dtxs)), nil
}

func (i *immuc) SQLQuery(args []string) (string, error) {
	sqlStmt, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.SQLQuery(ctx, &schema.SQLQueryRequest{Sql: string(sqlStmt), Limit: math.MaxInt32})
	})
	if err != nil {
		return "", err
	}

	txMetas := response.(*schema.SQLQueryResult)

	return fmt.Sprintf("sql ok: %v", txMetas), nil
}
