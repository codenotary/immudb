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

func (i *immuc) GetByIndex(args []string) (string, error) {
	index, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return "", fmt.Errorf(" \"%v\" is not a valid index number", args[0])
	}
	ctx := context.Background()
	response, err := i.ImmuClient.ByIndex(ctx, index)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return fmt.Sprintf("no item exists in index:%v", index), nil
		}
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}
	return PrintByIndex(response, i.valueOnly), nil
}

func (i *immuc) GetKey(args []string) (string, error) {
	key := []byte(args[0])
	ctx := context.Background()
	response, err := i.ImmuClient.Get(ctx, key)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return fmt.Sprintf("key not found: %v ", string(key)), nil
		}
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}

	return PrintItem([]byte(args[0]), nil, response, i.valueOnly), nil
}

func (i *immuc) RawSafeGetKey(args []string) (string, error) {
	key := []byte(args[0])
	ctx := context.Background()
	vi, err := i.ImmuClient.RawSafeGet(ctx, key)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return fmt.Sprintf("key not found: %v ", string(key)), nil
		}
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}

	return PrintItem(vi.Key, vi.Value, vi, i.valueOnly), nil
}

func (i *immuc) SafeGetKey(args []string) (string, error) {
	key := []byte(args[0])
	ctx := context.Background()
	response, err := i.ImmuClient.SafeGet(ctx, key)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return fmt.Sprintf("key not found: %v ", string(key)), nil
		}
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}
	return PrintItem([]byte(args[0]), nil, response, i.valueOnly), nil
}

func (i *immuc) GetRawBySafeIndex(args []string) (string, error) {
	index, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	response, err := i.ImmuClient.RawBySafeIndex(ctx, index)
	if err != nil {
		return "", err
	}
	resp := PrintItem(response.Key, response.Value, response, i.valueOnly)
	return resp, nil
}
