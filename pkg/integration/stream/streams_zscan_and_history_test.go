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

package integration

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/stretchr/testify/require"
)

func TestImmuClient_StreamZScan(t *testing.T) {
	_, client := setupTest(t)

	kvs := []*stream.KeyValue{}

	for i := 1; i <= 100; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		v := []byte(fmt.Sprintf("val-%d", i))
		kv := &stream.KeyValue{
			Key: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(k)),
				Size:    len(k),
			},
			Value: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(v)),
				Size:    len(v),
			},
		}
		kvs = append(kvs, kv)
	}

	hdr, err := client.StreamSet(context.Background(), kvs)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	set := "StreamZScanTestSet"
	setBytes := []byte(set)
	for i := range kvs {
		require.NoError(t, err)
		_, err = client.ZAdd(
			context.Background(), setBytes, float64((i+1)*10), []byte(fmt.Sprintf("key-%d", i+1)))
		require.NoError(t, err)
	}

	zScanResp, err := client.StreamZScan(context.Background(), &schema.ZScanRequest{Set: setBytes, SinceTx: hdr.Id})

	require.Len(t, zScanResp.Entries, 100)
}

func TestImmuClient_StreamHistory(t *testing.T) {
	_, client := setupTest(t)

	var hdr *schema.TxHeader
	var err error

	k := []byte("StreamHistoryTestKey")
	for i := 1; i <= 100; i++ {
		v := []byte(fmt.Sprintf("val-%d", i))
		kv := &stream.KeyValue{
			Key: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(k)),
				Size:    len(k),
			},
			Value: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(v)),
				Size:    len(v),
			},
		}
		hdr, err = client.StreamSet(context.Background(), []*stream.KeyValue{kv})
		require.NoError(t, err)
		require.NotNil(t, hdr)
	}

	historyResp, err := client.StreamHistory(context.Background(), &schema.HistoryRequest{Key: k, SinceTx: hdr.Id})
	require.NoError(t, err)

	require.Len(t, historyResp.Entries, 100)
}
