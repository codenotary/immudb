/*
Copyright 2019-2020 vChain, Inc.

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
	"fmt"
	"strconv"
	"strings"
)

func (i *immuc) ZScan(args []string) (string, error) {
	set := []byte(args[0])
	ctx := context.Background()
	response, err := i.ImmuClient.ZScan(ctx, set)
	if err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}
	str := strings.Builder{}
	if len(response.Items) == 0 {
		str.WriteString("0")
		return str.String(), nil
	}
	for _, item := range response.Items {
		str.WriteString(PrintItem(nil, nil, item, i.valueOnly))
		str.WriteString("\n")
	}
	return str.String(), nil
}

func (i *immuc) IScan(args []string) (string, error) {
	pageNumber, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return "", err
	}
	pageSize, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	response, err := i.ImmuClient.IScan(ctx, pageNumber, pageSize)
	if err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}

	str := strings.Builder{}
	if len(response.Items) == 0 {
		str.WriteString("0")
		return str.String(), nil
	}
	for _, item := range response.Items {
		str.WriteString(PrintItem(nil, nil, item, i.valueOnly))
		str.WriteString("\n")
	}
	return str.String(), nil
}

func (i *immuc) Scan(args []string) (string, error) {
	prefix := []byte(args[0])
	ctx := context.Background()
	response, err := i.ImmuClient.Scan(ctx, prefix)
	if err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}
	str := strings.Builder{}
	if len(response.Items) == 0 {
		str.WriteString("0")
		return str.String(), nil
	}
	for _, item := range response.Items {
		str.WriteString(PrintItem(nil, nil, item, i.valueOnly))
		str.WriteString("\n")
	}
	return str.String(), nil
}

func (i *immuc) Count(args []string) (string, error) {
	prefix := []byte(args[0])
	ctx := context.Background()
	response, err := i.ImmuClient.Count(ctx, prefix)
	if err != nil {
		return "", err
	}
	return fmt.Sprint(response.Count), nil
}
