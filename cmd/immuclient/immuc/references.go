/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) SetReference(args []string) (string, error) {
	var reader io.Reader

	if len(args) > 1 {
		reader = bytes.NewReader([]byte(args[1]))
	} else {
		reader = bufio.NewReader(os.Stdin)
	}

	key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	tee := io.TeeReader(reader, &buf)
	referencedKey, err := ioutil.ReadAll(tee)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.SetReference(ctx, key, referencedKey)
	})
	if err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}

	value, err := ioutil.ReadAll(&buf)
	if err != nil {
		return "", err
	}

	txhdr := response.(*schema.TxHeader)
	return PrintKV(
		&schema.Entry{
			Key:   []byte(args[0]),
			Value: value,
			Tx:    txhdr.Id,
		},
		false,
		false,
	), nil
}

func (i *immuc) VerifiedSetReference(args []string) (string, error) {
	var reader io.Reader

	if len(args) > 1 {
		reader = bytes.NewReader([]byte(args[1]))
	} else {
		reader = bufio.NewReader(os.Stdin)
	}

	key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	tee := io.TeeReader(reader, &buf)
	referencedKey, err := ioutil.ReadAll(tee)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.VerifiedSetReference(ctx, key, referencedKey)
	})
	if err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}

	value, err := ioutil.ReadAll(&buf)
	if err != nil {
		return "", err
	}

	txhdr := response.(*schema.TxHeader)
	return PrintKV(
		&schema.Entry{
			Key:   []byte(args[0]),
			Value: value,
			Tx:    txhdr.Id,
		},
		true,
		false,
	), nil
}
