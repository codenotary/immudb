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

	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) HealthCheck(args []string) (CommandOutput, error) {
	ctx := context.Background()

	if _, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return nil, immuClient.HealthCheck(ctx)
	}); err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")

		if len(rpcerrors) > 1 {
			return &errorOutput{
				err: rpcerrors[len(rpcerrors)-1],
			}, nil
		}

		return nil, err
	}

	return &resultOutput{
		Result: "Health check OK",
	}, nil
}
