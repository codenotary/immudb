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
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) History(args []string) (string, error) {
	key := []byte(args[0])
	ctx := context.Background()

	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.History(ctx, &schema.HistoryRequest{
			Key: key,
		})
	})
	if err != nil {
		rpcerrors := strings.SplitAfter(err.Error(), "=")
		if len(rpcerrors) > 1 {
			return rpcerrors[len(rpcerrors)-1], nil
		}
		return "", err
	}

	str := strings.Builder{}

	entries := response.(*schema.Entries)
	if len(entries.Entries) == 0 {
		str.WriteString("No item found \n")
		return str.String(), nil
	}

	for j, entry := range entries.Entries {
		if j > 0 {
			str.WriteString("\n")
		}
		str.WriteString(PrintKV(entry, false, false))
	}

	return str.String(), nil
}
