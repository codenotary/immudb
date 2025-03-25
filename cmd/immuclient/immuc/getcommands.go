/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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

func (i *immuc) parseKeyArg(arg string) (key []byte, revision int64, hasRevision bool, err error) {
	if i.options.revisionSeparator == "" {
		// No revision separator - argument is the key
		return []byte(arg), 0, false, nil
	}

	idx := strings.LastIndex(arg, i.options.revisionSeparator)
	if idx < 0 {
		// No revision separator in the argument - that's a key without revision
		return []byte(arg), 0, false, nil
	}

	key = []byte(arg[:idx])
	revisionStr := arg[idx+len(i.options.revisionSeparator):]

	revision, err = strconv.ParseInt(revisionStr, 10, 64)
	if err != nil {
		return nil, 0, false, fmt.Errorf("Invalid key revision number - not an integer: %w", err)
	}

	return key, revision, true, nil
}

func (i *immuc) Get(args []string) (string, error) {
	key, atRevision, _, err := i.parseKeyArg(args[0])
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.Get(ctx, key, client.AtRevision(atRevision))
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
	return PrintKV(entry, false, i.options.valueOnly), nil
}

func (i *immuc) VerifiedGet(args []string) (string, error) {
	key, atRevision, _, err := i.parseKeyArg(args[0])
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.VerifiedGet(ctx, key, client.AtRevision(atRevision))
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
	return PrintKV(entry, true, i.options.valueOnly), nil
}
