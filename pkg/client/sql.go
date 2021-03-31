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
)

func (c *immuClient) SQLExec(ctx context.Context, req *schema.SQLExecRequest) (*schema.SQLExecResult, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.SQLExec(ctx, req)
}

func (c *immuClient) SQLQuery(ctx context.Context, req *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.SQLQuery(ctx, req)
}
