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

package database

import (
	"strconv"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyReplica(t *testing.T) {
	rootPath := "data_" + strconv.FormatInt(time.Now().UnixNano(), 10)

	ropts := &ReplicationOptions{Replica: true}
	options := DefaultOption().WithDbRootPath(rootPath).WithDbName("db").WithReplicationOptions(ropts)

	replica, rcloser := makeDbWith(options)
	defer rcloser()

	_, err := replica.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte("key1"), Value: []byte("value1")}}})
	require.Equal(t, ErrIsReplica, err)

	_, err = replica.ExecAll(&schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
				},
			},
		}},
	)
	require.Equal(t, ErrIsReplica, err)

	_, err = replica.SetReference(&schema.ReferenceRequest{
		Key:           []byte("key"),
		ReferencedKey: []byte("refkey"),
	})
	require.Equal(t, ErrIsReplica, err)

	_, err = replica.ZAdd(&schema.ZAddRequest{
		Set:   []byte("set"),
		Score: 1,
		Key:   []byte("key"),
	})
	require.Equal(t, ErrIsReplica, err)
}
