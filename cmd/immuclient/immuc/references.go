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
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

func (i *immuc) Reference(args []string) (string, error) {
	var reader io.Reader
	if len(args) > 1 {
		reader = bytes.NewReader([]byte(args[1]))
	} else {
		reader = bufio.NewReader(os.Stdin)
	}
	reference, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	tee := io.TeeReader(reader, &buf)
	key, err := ioutil.ReadAll(tee)
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	response, err := i.ImmuClient.Reference(ctx, reference, key)
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
	return PrintItem([]byte(args[0]), value, response, false), nil
}

func (i *immuc) SafeReference(args []string) (string, error) {
	var reader io.Reader
	if len(args) > 1 {
		reader = bytes.NewReader([]byte(args[1]))
	} else {
		reader = bufio.NewReader(os.Stdin)
	}
	reference, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	tee := io.TeeReader(reader, &buf)
	key, err := ioutil.ReadAll(tee)
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	response, err := i.ImmuClient.SafeReference(ctx, reference, key)
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
	return PrintItem([]byte(args[0]), value, response, false), nil
}
