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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyReplica(t *testing.T) {
	rootPath := "data_" + strconv.FormatInt(time.Now().UnixNano(), 10)

	options := DefaultOption().WithDBRootPath(rootPath).WithDBName("db").AsReplica(true)

	replica, err := NewDB(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	defer os.RemoveAll(options.dbRootPath)

	err = replica.Close()
	require.NoError(t, err)

	replica, err = OpenDB(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	_, err = replica.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte("key1"), Value: []byte("value1")}}})
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

	_, err = replica.SQLExec(&schema.SQLExecRequest{Sql: "CREATE TABLE mytable(id INTEGER, title VARCHAR, PRIMARY KEY id)"})
	require.Equal(t, ErrIsReplica, err)

	_, err = replica.SQLQuery(&schema.SQLQueryRequest{Sql: "SELECT * FROM mytable"})
	require.Equal(t, ErrSQLNotReady, err)

	_, err = replica.ListTables()
	require.Equal(t, ErrSQLNotReady, err)

	_, err = replica.DescribeTable("mytable")
	require.Equal(t, ErrSQLNotReady, err)

	err = replica.UseSnapshot(&schema.UseSnapshotRequest{SinceTx: 1})
	require.Equal(t, ErrSQLNotReady, err)

	_, err = replica.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "mytable",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
		},
	})
	require.Equal(t, ErrSQLNotReady, err)
}

func TestSwitchToReplica(t *testing.T) {
	rootPath := "data_" + strconv.FormatInt(time.Now().UnixNano(), 10)

	options := DefaultOption().WithDBRootPath(rootPath).WithDBName("db").AsReplica(false)

	replica, rcloser := makeDbWith(options)
	defer rcloser()

	_, err := replica.SQLExec(&schema.SQLExecRequest{Sql: "CREATE TABLE mytable(id INTEGER, title VARCHAR, PRIMARY KEY id)"})
	require.NoError(t, err)

	_, err = replica.SQLExec(&schema.SQLExecRequest{Sql: "INSERT INTO mytable(id, title) VALUES (1, 'TITLE1')"})
	require.NoError(t, err)

	replica.AsReplica(true)

	err = replica.UseSnapshot(&schema.UseSnapshotRequest{SinceTx: 1})
	require.NoError(t, err)

	_, err = replica.ListTables()
	require.NoError(t, err)

	_, err = replica.DescribeTable("mytable")
	require.NoError(t, err)

	_, err = replica.SQLQuery(&schema.SQLQueryRequest{Sql: "SELECT * FROM mytable"})
	require.NoError(t, err)

	_, err = replica.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "mytable",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
		},
	})
	require.NoError(t, err)
}
