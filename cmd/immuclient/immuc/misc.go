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
	"context"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) History(args []string) (string, error) {
	key := []byte(args[0])
	ctx := context.Background()

	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.History(ctx, &schema.HistoryRequest{
			Key: key,
		})
	})
	if err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}

	str := strings.Builder{}

	entries := response.(*schema.Entries)
	if len(entries.Entries) == 0 {
		str.WriteString("No item found \n")
		return str.String(), nil
	}

	for _, entry := range entries.Entries {
		str.WriteString(PrintKV(entry.Key, entry.Value, entry.Tx, false, false))
		str.WriteString("\n")
	}

	return str.String(), nil
}

func (i *immuc) HealthCheck(args []string) (string, error) {
	ctx := context.Background()

	if _, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return nil, immuClient.HealthCheck(ctx)
	}); err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")

		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}

		return "", err
	}

	return "Health check OK", nil
}
