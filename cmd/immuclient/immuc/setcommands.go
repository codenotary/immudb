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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) Set(args []string) (CommandOutput, error) {
	var reader io.Reader

	if len(args) > 1 {
		reader = bytes.NewReader([]byte(args[1]))
	} else {
		reader = bufio.NewReader(os.Stdin)
	}

	key := []byte(args[0])

	value, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	response, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.Set(ctx, key, value)
	})
	if err != nil {
		return nil, err
	}

	txhdr := response.(*schema.TxHeader)
	scstr, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.GetSince(ctx, key, txhdr.Id)
	})
	if err != nil {
		return nil, err
	}

	return &kvOutput{
		entry: scstr.(*schema.Entry),
	}, nil
}

func (i *immuc) VerifiedSet(args []string) (CommandOutput, error) {
	var reader io.Reader

	if len(args) > 1 {
		reader = bytes.NewReader([]byte(args[1]))
	} else {
		reader = bufio.NewReader(os.Stdin)
	}

	key := []byte(args[0])

	value, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	if _, err = i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.VerifiedSet(ctx, key, value)
	}); err != nil {
		return nil, err
	}

	vi, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.VerifiedGet(ctx, key)
	})
	if err != nil {
		return nil, err
	}

	return &kvOutput{
		entry:    vi.(*schema.Entry),
		verified: true,
	}, nil
}

func (i *immuc) Restore(args []string) (CommandOutput, error) {
	key, atRevision, hasRevision, err := i.parseKeyArg(args[0])
	if err != nil {
		return nil, err
	}

	if !hasRevision {
		return &errorOutput{err: "please specify the key with revision to restore"}, nil
	}

	if atRevision == 0 {
		return &errorOutput{err: "can not restore current revision"}, nil
	}

	ctx := context.Background()
	oldValue, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.Get(ctx, key, client.AtRevision(atRevision))
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return &errorOutput{err: fmt.Sprintf("key not found: %v ", string(key))}, nil
		}
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return &errorOutput{err: rpcerrors[len(rpcerrors)-1]}, nil
		}
		return nil, err
	}

	oldEntry := oldValue.(*schema.Entry)

	newValue, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.SetAll(ctx, &schema.SetRequest{
			KVs: []*schema.KeyValue{{
				Key:   oldEntry.Key,
				Value: oldEntry.Value,
			}},
		})
	})
	if err != nil {
		return nil, err
	}

	txhdr := newValue.(*schema.TxHeader)
	scstr, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.GetSince(ctx, key, txhdr.Id)
	})
	if err != nil {
		return nil, err
	}

	return &kvOutput{
		entry: scstr.(*schema.Entry),
	}, nil
}

func (i *immuc) DeleteKey(args []string) (CommandOutput, error) {
	key := []byte(args[0])
	ctx := context.Background()
	_, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.Delete(ctx, &schema.DeleteKeysRequest{Keys: [][]byte{key}})
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return &errorOutput{err: fmt.Sprintf("key not found: %v ", string(key))}, nil
		}
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return &errorOutput{err: rpcerrors[len(rpcerrors)-1]}, nil
		}
		return nil, err
	}

	return &resultOutput{Result: "key successfully deleted"}, nil
}

func (i *immuc) ZAdd(args []string) (CommandOutput, error) {
	var setReader io.Reader
	var scoreReader io.Reader
	var keyReader io.Reader

	if len(args) > 1 {
		setReader = bytes.NewReader([]byte(args[0]))
		scoreReader = bytes.NewReader([]byte(args[1]))
		keyReader = bytes.NewReader([]byte(args[2]))
	}

	bs, err := ioutil.ReadAll(scoreReader)
	if err != nil {
		return nil, err
	}

	score, err := strconv.ParseFloat(string(bs), 64)
	if err != nil {
		return nil, err
	}

	set, err := ioutil.ReadAll(setReader)
	if err != nil {
		return nil, err
	}

	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	txhdr, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.ZAdd(ctx, set, score, key)
	})
	if err != nil {
		return nil, err
	}

	return &zEntryOutput{
		set:           set,
		referencedKey: key,
		score:         score,
		txhdr:         txhdr.(*schema.TxHeader),
		verified:      false,
	}, nil
}

func (i *immuc) VerifiedZAdd(args []string) (CommandOutput, error) {
	var setReader io.Reader
	var scoreReader io.Reader
	var keyReader io.Reader

	if len(args) > 1 {
		setReader = bytes.NewReader([]byte(args[0]))
		scoreReader = bytes.NewReader([]byte(args[1]))
		keyReader = bytes.NewReader([]byte(args[2]))
	}

	bs, err := ioutil.ReadAll(scoreReader)
	if err != nil {
		return nil, err
	}

	score, err := strconv.ParseFloat(string(bs), 64)
	if err != nil {
		return nil, err
	}

	set, err := ioutil.ReadAll(setReader)
	if err != nil {
		return nil, err
	}

	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	response, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.VerifiedZAdd(ctx, set, score, key)
	})
	if err != nil {
		return nil, err
	}

	return &zEntryOutput{
		set:           set,
		referencedKey: key,
		score:         score,
		txhdr:         response.(*schema.TxHeader),
		verified:      true,
	}, nil
}
