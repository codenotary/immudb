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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

var (
	errZeroTxID = errors.New("tx id cannot be 0 (should be bigger than 0)")
)

func (i *immuc) GetTxByID(args []string) (string, error) {
	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return "", fmt.Errorf(" \"%v\" is not a valid id number", args[0])
	}
	if id == 0 {
		return "", errZeroTxID
	}

	ctx := context.Background()
	tx, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.TxByID(ctx, id)
	})
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
	return PrintTx(tx.(*schema.Tx), false), nil
}

func (i *immuc) VerifiedGetTxByID(args []string) (string, error) {
	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return "", fmt.Errorf(" \"%v\" is not a valid id number", args[0])
	}
	if id == 0 {
		return "", errZeroTxID
	}

	ctx := context.Background()
	tx, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.VerifiedTxByID(ctx, id)
	})
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
	return PrintTx(tx.(*schema.Tx), true), nil
}

func (i *immuc) Get(args []string) (string, error) {
	key := []byte(args[0])
	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.Get(ctx, key)
	})
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

	entry := response.(*schema.Entry)
	return PrintKV(entry.Key, entry.Metadata, entry.Value, entry.Tx, false, i.valueOnly), nil
}

func (i *immuc) VerifiedGet(args []string) (string, error) {
	key := []byte(args[0])
	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.VerifiedGet(ctx, key)
	})
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

	entry := response.(*schema.Entry)
	return PrintKV(entry.Key, entry.Metadata, entry.Value, entry.Tx, true, i.valueOnly), nil
}
