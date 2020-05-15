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

package cli

import (
	"bytes"
	"context"
	"io/ioutil"
	"strings"
)

func (cli *cli) history(args []string) (string, error) {
	key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	response, err := cli.ImmuClient.History(ctx, key)
	if err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}
	str := strings.Builder{}
	if len(response.Items) == 0 {
		str.WriteString("No item found \n")
		return str.String(), nil
	}
	for _, item := range response.Items {
		str.WriteString(printItem(nil, nil, item, false))
		str.WriteString("\n")
	}
	return str.String(), nil
}

func (cli *cli) healthCheck(args []string) (string, error) {
	ctx := context.Background()
	if err := cli.ImmuClient.HealthCheck(ctx); err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}
	return "Health check OK", nil
}
