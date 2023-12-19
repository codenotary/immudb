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
	"context"
	"strings"

	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) HealthCheck(args []string) (string, error) {
	ctx := context.Background()

	if _, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return nil, immuClient.HealthCheck(ctx)
	}); err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")

		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}

		return "", err
	}

	return "Health check OK", nil
}
