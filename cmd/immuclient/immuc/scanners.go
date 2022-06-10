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

func (i *immuc) ZScan(args []string) (CommandOutput, error) {
	set := []byte(args[0])
	ctx := context.Background()

	response, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.ZScan(ctx, &schema.ZScanRequest{Set: set, NoWait: true})
	})
	if err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")

		if len(rpcerrors) > 1 {
			return &errorOutput{err: rpcerrors[len(rpcerrors)-1]}, nil
		}

		return nil, err
	}

	zEntries := response.(*schema.ZEntries)
	if len(zEntries.Entries) == 0 {
		return &errorOutput{err: "no entries"}, nil
	}

	resp := &multiKVOutput{
		entries: make([]kvOutput, 0, len(zEntries.Entries)),
	}

	for _, entry := range zEntries.Entries {
		resp.entries = append(resp.entries, kvOutput{
			entry: entry.Entry,
		})
	}

	return resp, nil
}

func (i *immuc) Scan(args []string) (CommandOutput, error) {
	prefix := []byte(args[0])

	ctx := context.Background()

	response, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.Scan(ctx, &schema.ScanRequest{Prefix: prefix, NoWait: true})
	})
	if err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return &errorOutput{err: rpcerrors[len(rpcerrors)-1]}, nil
		}
		return nil, err
	}

	entries := response.(*schema.Entries)
	if len(entries.Entries) == 0 {
		return &errorOutput{err: "no entries"}, nil
	}

	ret := &multiKVOutput{
		entries: make([]kvOutput, 0, len(entries.Entries)),
	}
	for _, entry := range entries.Entries {
		ret.entries = append(ret.entries, kvOutput{
			entry: entry,
		})
	}

	return ret, nil
}

func (i *immuc) Count(args []string) (CommandOutput, error) {
	prefix := []byte(args[0])
	ctx := context.Background()

	response, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.Count(ctx, prefix)
	})
	if err != nil {
		return nil, err
	}

	return &resultOutput{Result: response.(*schema.EntryCount).Count}, nil
}
