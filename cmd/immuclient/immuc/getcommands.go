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

func (i *immuc) GetTxByID(args []string) (string, error) {
	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return "", fmt.Errorf(" \"%v\" is not a valid id number", args[0])
	}
	ctx := context.Background()
	tx, err := i.ImmuClient.TxByID(ctx, id)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return fmt.Sprintf("no item exists in id:%v", id), nil
		}
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}
	return PrintTx(tx, false), nil
}

func (i *immuc) VerifiedGetTxByID(args []string) (string, error) {
	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return "", fmt.Errorf(" \"%v\" is not a valid id number", args[0])
	}
	ctx := context.Background()
	tx, err := i.ImmuClient.VerifiedTxByID(ctx, id)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return fmt.Sprintf("no item exists in id:%v", id), nil
		}
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}
	return PrintTx(tx, true), nil
}

func (i *immuc) Get(args []string) (string, error) {
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

	return PrintKV([]byte(args[0]), response.Value, response.Tx, false, i.valueOnly), nil
}

func (i *immuc) VerifiedGet(args []string) (string, error) {
	key := []byte(args[0])
	ctx := context.Background()
	response, err := i.ImmuClient.VerifiedGet(ctx, key)
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
	return PrintKV([]byte(args[0]), response.Value, response.Tx, true, i.valueOnly), nil
}
