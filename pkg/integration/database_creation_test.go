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
		DatabaseName:            "db1",
		Replica:                 false,
		FileSize:                1 << 20,
		MaxKeyLen:               32,
		MaxValueLen:             64,
		MaxTxEntries:            100,
		ExcludeCommitTime:       false,
		MaxConcurrency:          10,
		MaxIOConcurrency:        2,
		TxLogCacheSize:          2000,
		VLogMaxOpenedFiles:      8,
		TxLogMaxOpenedFiles:     4,
		CommitLogMaxOpenedFiles: 2,
		IndexSettings: &schema.IndexSettings{
			Synced:                false,
			FlushThreshold:        256,
			SyncThreshold:         512,
			CacheSize:             1024,
			MaxNodeSize:           8192,
			MaxActiveSnapshots:    3,
			RenewSnapRootAfter:    5000,
			CompactionThld:        5,
			DelayDuringCompaction: 1,
		},
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
	require.Equal(t, dbSettings.MaxConcurrency, settings.MaxConcurrency)
	require.Equal(t, dbSettings.MaxIOConcurrency, settings.MaxIOConcurrency)
	require.Equal(t, dbSettings.TxLogCacheSize, settings.TxLogCacheSize)
	require.Equal(t, dbSettings.VLogMaxOpenedFiles, settings.VLogMaxOpenedFiles)
	require.Equal(t, dbSettings.TxLogMaxOpenedFiles, settings.TxLogMaxOpenedFiles)
	require.Equal(t, dbSettings.CommitLogMaxOpenedFiles, settings.CommitLogMaxOpenedFiles)

	require.Equal(t, dbSettings.IndexSettings.Synced, settings.IndexSettings.Synced)
	require.Equal(t, dbSettings.IndexSettings.FlushThreshold, settings.IndexSettings.FlushThreshold)
	require.Equal(t, dbSettings.IndexSettings.SyncThreshold, settings.IndexSettings.SyncThreshold)
	require.Equal(t, dbSettings.IndexSettings.CacheSize, settings.IndexSettings.CacheSize)
	require.Equal(t, dbSettings.IndexSettings.MaxNodeSize, settings.IndexSettings.MaxNodeSize)
	require.Equal(t, dbSettings.IndexSettings.MaxActiveSnapshots, settings.IndexSettings.MaxActiveSnapshots)
	require.Equal(t, dbSettings.IndexSettings.RenewSnapRootAfter, settings.IndexSettings.RenewSnapRootAfter)
	require.Equal(t, dbSettings.IndexSettings.CompactionThld, settings.IndexSettings.CompactionThld)
	require.Equal(t, dbSettings.IndexSettings.DelayDuringCompaction, settings.IndexSettings.DelayDuringCompaction)
}
