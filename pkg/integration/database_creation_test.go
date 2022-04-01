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
package integration

import (
	"context"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestCreateDatabase(t *testing.T) {
	options := server.DefaultOptions()
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	clientOpts := immudb.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})
	client := immudb.NewClient().WithOptions(clientOpts)

	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	dbSettings := &schema.DatabaseSettings{
		DatabaseName:      "db1",
		Replica:           false,
		FileSize:          1 << 20,
		MaxKeyLen:         32,
		MaxValueLen:       64,
		MaxTxEntries:      100,
		ExcludeCommitTime: false,
	}
	err = client.CreateDatabase(context.Background(), dbSettings)
	require.NoError(t, err)

	_, err = client.UseDatabase(context.Background(), &schema.Database{DatabaseName: "db1"})
	require.NoError(t, err)

	settings, err := client.GetDatabaseSettings(context.Background())
	require.NoError(t, err)
	require.Equal(t, dbSettings.DatabaseName, settings.DatabaseName)
	require.Equal(t, dbSettings.Replica, settings.Replica)
	require.Equal(t, dbSettings.FileSize, settings.FileSize)
	require.Equal(t, dbSettings.MaxKeyLen, settings.MaxKeyLen)
	require.Equal(t, dbSettings.MaxValueLen, settings.MaxValueLen)
	require.Equal(t, dbSettings.MaxTxEntries, settings.MaxTxEntries)
	require.Equal(t, dbSettings.ExcludeCommitTime, settings.ExcludeCommitTime)
}

func TestCreateDatabaseV2(t *testing.T) {
	options := server.DefaultOptions()
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	clientOpts := immudb.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})
	client := immudb.NewClient().WithOptions(clientOpts)

	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	dbNullableSettings := &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica: &schema.NullableBool{Value: false},
		},
		FileSize:                &schema.NullableUint32{Value: 1 << 20},
		MaxKeyLen:               &schema.NullableUint32{Value: 32},
		MaxValueLen:             &schema.NullableUint32{Value: 64},
		MaxTxEntries:            &schema.NullableUint32{Value: 100},
		ExcludeCommitTime:       &schema.NullableBool{Value: false},
		MaxConcurrency:          &schema.NullableUint32{Value: 10},
		MaxIOConcurrency:        &schema.NullableUint32{Value: 2},
		TxLogCacheSize:          &schema.NullableUint32{Value: 2000},
		VLogMaxOpenedFiles:      &schema.NullableUint32{Value: 8},
		TxLogMaxOpenedFiles:     &schema.NullableUint32{Value: 4},
		CommitLogMaxOpenedFiles: &schema.NullableUint32{Value: 2},
		IndexSettings: &schema.IndexNullableSettings{
			FlushThreshold:           &schema.NullableUint32{Value: 256},
			SyncThreshold:            &schema.NullableUint32{Value: 512},
			FlushBufferSize:          &schema.NullableUint32{Value: 128},
			CacheSize:                &schema.NullableUint32{Value: 1024},
			MaxNodeSize:              &schema.NullableUint32{Value: 8192},
			MaxActiveSnapshots:       &schema.NullableUint32{Value: 3},
			RenewSnapRootAfter:       &schema.NullableUint64{Value: 5000},
			CompactionThld:           &schema.NullableUint32{Value: 5},
			DelayDuringCompaction:    &schema.NullableUint32{Value: 1},
			NodesLogMaxOpenedFiles:   &schema.NullableUint32{Value: 20},
			HistoryLogMaxOpenedFiles: &schema.NullableUint32{Value: 15},
			CommitLogMaxOpenedFiles:  &schema.NullableUint32{Value: 3},
		},
	}
	_, err = client.CreateDatabaseV2(context.Background(), "db1", dbNullableSettings)
	require.NoError(t, err)

	_, err = client.UseDatabase(context.Background(), &schema.Database{DatabaseName: "db1"})
	require.NoError(t, err)

	settings, err := client.GetDatabaseSettingsV2(context.Background())
	require.NoError(t, err)
	require.Equal(t, dbNullableSettings.ReplicationSettings.Replica.Value, settings.ReplicationSettings.Replica.Value)
	require.Equal(t, dbNullableSettings.FileSize.Value, settings.FileSize.Value)
	require.Equal(t, dbNullableSettings.MaxKeyLen.Value, settings.MaxKeyLen.Value)
	require.Equal(t, dbNullableSettings.MaxValueLen.Value, settings.MaxValueLen.Value)
	require.Equal(t, dbNullableSettings.MaxTxEntries.Value, settings.MaxTxEntries.Value)
	require.Equal(t, dbNullableSettings.ExcludeCommitTime.Value, settings.ExcludeCommitTime.Value)
	require.Equal(t, dbNullableSettings.MaxConcurrency.Value, settings.MaxConcurrency.Value)
	require.Equal(t, dbNullableSettings.MaxIOConcurrency.Value, settings.MaxIOConcurrency.Value)
	require.Equal(t, dbNullableSettings.TxLogCacheSize.Value, settings.TxLogCacheSize.Value)
	require.Equal(t, dbNullableSettings.VLogMaxOpenedFiles.Value, settings.VLogMaxOpenedFiles.Value)
	require.Equal(t, dbNullableSettings.TxLogMaxOpenedFiles.Value, settings.TxLogMaxOpenedFiles.Value)
	require.Equal(t, dbNullableSettings.CommitLogMaxOpenedFiles.Value, settings.CommitLogMaxOpenedFiles.Value)

	require.Equal(t, dbNullableSettings.IndexSettings.FlushThreshold.Value, settings.IndexSettings.FlushThreshold.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.SyncThreshold.Value, settings.IndexSettings.SyncThreshold.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.FlushBufferSize.Value, settings.IndexSettings.FlushBufferSize.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.CacheSize.Value, settings.IndexSettings.CacheSize.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.MaxNodeSize.Value, settings.IndexSettings.MaxNodeSize.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.MaxActiveSnapshots.Value, settings.IndexSettings.MaxActiveSnapshots.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.RenewSnapRootAfter.Value, settings.IndexSettings.RenewSnapRootAfter.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.CompactionThld.Value, settings.IndexSettings.CompactionThld.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.DelayDuringCompaction.Value, settings.IndexSettings.DelayDuringCompaction.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.NodesLogMaxOpenedFiles.Value, settings.IndexSettings.NodesLogMaxOpenedFiles.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.HistoryLogMaxOpenedFiles.Value, settings.IndexSettings.HistoryLogMaxOpenedFiles.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.CommitLogMaxOpenedFiles.Value, settings.IndexSettings.CommitLogMaxOpenedFiles.Value)
}
