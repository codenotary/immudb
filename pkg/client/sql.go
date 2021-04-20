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
package client

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (c *immuClient) SQLExec(ctx context.Context, sql string, params map[string]interface{}) (*schema.SQLExecResult, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	namedParams, err := encodeParams(params)
	if err != nil {
		return nil, err
	}

	return c.ServiceClient.SQLExec(ctx, &schema.SQLExecRequest{Sql: sql, Params: namedParams})
}

func (c *immuClient) SQLQuery(ctx context.Context, sql string, params map[string]interface{}) (*schema.SQLQueryResult, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	namedParams, err := encodeParams(params)
	if err != nil {
		return nil, err
	}

	return c.ServiceClient.SQLQuery(ctx, &schema.SQLQueryRequest{Sql: sql, Params: namedParams})
}

func (c *immuClient) ListTables(ctx context.Context) (*schema.SQLQueryResult, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.ListTables(ctx, &emptypb.Empty{})
}

func (c *immuClient) DescribeTable(ctx context.Context, tableName string) (*schema.SQLQueryResult, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.DescribeTable(ctx, &schema.Table{TableName: tableName})
}

func encodeParams(params map[string]interface{}) ([]*schema.NamedParam, error) {
	if params == nil {
		return nil, nil
	}

	namedParams := make([]*schema.NamedParam, len(params))

	i := 0
	for n, v := range params {
		var val *schema.SQLValue

		if v == nil {
			val = &schema.SQLValue{Value: &schema.SQLValue_Null{}}
		}

		switch tv := v.(type) {
		case int64:
			{
				val = &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}
			}
		case uint64:
			{
				val = &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}
			}
		case string:
			{
				val = &schema.SQLValue{Value: &schema.SQLValue_S{S: tv}}
			}
		case bool:
			{
				val = &schema.SQLValue{Value: &schema.SQLValue_B{B: tv}}
			}
		case []byte:
			{
				val = &schema.SQLValue{Value: &schema.SQLValue_Bs{Bs: tv}}
			}
		}

		namedParams[i] = &schema.NamedParam{Name: n, Value: val}

		i++
	}

	return namedParams, nil
}
