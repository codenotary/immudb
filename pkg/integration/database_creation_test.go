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

package integration

import (
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestCreateDatabase(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	dbSettings := &schema.DatabaseSettings{
		DatabaseName:      "db1",
		Replica:           false,
		FileSize:          1 << 20,
		MaxKeyLen:         32,
		MaxValueLen:       64,
		MaxTxEntries:      100,
		ExcludeCommitTime: false,
	}
	err := client.CreateDatabase(ctx, dbSettings)
	require.NoError(t, err)

	_, err = client.UseDatabase(ctx, &schema.Database{DatabaseName: "db1"})
	require.NoError(t, err)

	settings, err := client.GetDatabaseSettings(ctx)
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
	_, client, ctx := setupTestServerAndClient(t)

	dbNullableSettings := &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica: &schema.NullableBool{Value: false},
		},
		FileSize:                &schema.NullableUint32{Value: 1 << 20},
		MaxKeyLen:               &schema.NullableUint32{Value: 32},
		MaxValueLen:             &schema.NullableUint32{Value: 64},
		MaxTxEntries:            &schema.NullableUint32{Value: 100},
		EmbeddedValues:          &schema.NullableBool{Value: true},
		PreallocFiles:           &schema.NullableBool{Value: true},
		ExcludeCommitTime:       &schema.NullableBool{Value: false},
		MaxActiveTransactions:   &schema.NullableUint32{Value: 30},
		MvccReadSetLimit:        &schema.NullableUint32{Value: 1_000},
		MaxConcurrency:          &schema.NullableUint32{Value: 10},
		MaxIOConcurrency:        &schema.NullableUint32{Value: 1},
		TxLogCacheSize:          &schema.NullableUint32{Value: 2000},
		VLogCacheSize:           &schema.NullableUint32{Value: 2200},
		VLogMaxOpenedFiles:      &schema.NullableUint32{Value: 8},
		TxLogMaxOpenedFiles:     &schema.NullableUint32{Value: 4},
		CommitLogMaxOpenedFiles: &schema.NullableUint32{Value: 2},
		SyncFrequency:           &schema.NullableMilliseconds{Value: 15},
		WriteBufferSize:         &schema.NullableUint32{Value: 4000},
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
			MaxBulkSize:              &schema.NullableUint32{Value: 35},
			BulkPreparationTimeout:   &schema.NullableMilliseconds{Value: 150},
		},
		AhtSettings: &schema.AHTNullableSettings{
			SyncThreshold:   &schema.NullableUint32{Value: 10_000},
			WriteBufferSize: &schema.NullableUint32{Value: 8000},
		},
		TruncationSettings: &schema.TruncationNullableSettings{
			RetentionPeriod:     &schema.NullableMilliseconds{Value: 24 * time.Hour.Milliseconds()},
			TruncationFrequency: &schema.NullableMilliseconds{Value: 1 * time.Hour.Milliseconds()},
		},
	}
	_, err := client.CreateDatabaseV2(ctx, "db1", dbNullableSettings)
	require.NoError(t, err)

	_, err = client.UseDatabase(ctx, &schema.Database{DatabaseName: "db1"})
	require.NoError(t, err)

	res, err := client.GetDatabaseSettingsV2(ctx)
	require.NoError(t, err)
	require.Equal(t, dbNullableSettings.ReplicationSettings.Replica.Value, res.Settings.ReplicationSettings.Replica.Value)
	require.Equal(t, dbNullableSettings.FileSize.Value, res.Settings.FileSize.Value)
	require.Equal(t, dbNullableSettings.MaxKeyLen.Value, res.Settings.MaxKeyLen.Value)
	require.Equal(t, dbNullableSettings.MaxValueLen.Value, res.Settings.MaxValueLen.Value)
	require.Equal(t, dbNullableSettings.MaxTxEntries.Value, res.Settings.MaxTxEntries.Value)
	require.Equal(t, dbNullableSettings.EmbeddedValues.Value, res.Settings.EmbeddedValues.Value)
	require.Equal(t, dbNullableSettings.PreallocFiles.Value, res.Settings.PreallocFiles.Value)
	require.Equal(t, dbNullableSettings.ExcludeCommitTime.Value, res.Settings.ExcludeCommitTime.Value)
	require.Equal(t, dbNullableSettings.MaxActiveTransactions.Value, res.Settings.MaxActiveTransactions.Value)
	require.Equal(t, dbNullableSettings.MvccReadSetLimit.Value, res.Settings.MvccReadSetLimit.Value)
	require.Equal(t, dbNullableSettings.MaxConcurrency.Value, res.Settings.MaxConcurrency.Value)
	require.Equal(t, dbNullableSettings.MaxIOConcurrency.Value, res.Settings.MaxIOConcurrency.Value)
	require.Equal(t, dbNullableSettings.TxLogCacheSize.Value, res.Settings.TxLogCacheSize.Value)
	require.Equal(t, dbNullableSettings.VLogCacheSize.Value, res.Settings.VLogCacheSize.Value)
	require.Equal(t, dbNullableSettings.VLogMaxOpenedFiles.Value, res.Settings.VLogMaxOpenedFiles.Value)
	require.Equal(t, dbNullableSettings.TxLogMaxOpenedFiles.Value, res.Settings.TxLogMaxOpenedFiles.Value)
	require.Equal(t, dbNullableSettings.CommitLogMaxOpenedFiles.Value, res.Settings.CommitLogMaxOpenedFiles.Value)
	require.Equal(t, dbNullableSettings.SyncFrequency.Value, res.Settings.SyncFrequency.Value)
	require.Equal(t, dbNullableSettings.WriteBufferSize.Value, res.Settings.WriteBufferSize.Value)

	require.Equal(t, dbNullableSettings.IndexSettings.FlushThreshold.Value, res.Settings.IndexSettings.FlushThreshold.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.SyncThreshold.Value, res.Settings.IndexSettings.SyncThreshold.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.FlushBufferSize.Value, res.Settings.IndexSettings.FlushBufferSize.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.CacheSize.Value, res.Settings.IndexSettings.CacheSize.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.MaxNodeSize.Value, res.Settings.IndexSettings.MaxNodeSize.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.MaxActiveSnapshots.Value, res.Settings.IndexSettings.MaxActiveSnapshots.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.RenewSnapRootAfter.Value, res.Settings.IndexSettings.RenewSnapRootAfter.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.CompactionThld.Value, res.Settings.IndexSettings.CompactionThld.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.DelayDuringCompaction.Value, res.Settings.IndexSettings.DelayDuringCompaction.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.NodesLogMaxOpenedFiles.Value, res.Settings.IndexSettings.NodesLogMaxOpenedFiles.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.HistoryLogMaxOpenedFiles.Value, res.Settings.IndexSettings.HistoryLogMaxOpenedFiles.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.CommitLogMaxOpenedFiles.Value, res.Settings.IndexSettings.CommitLogMaxOpenedFiles.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.MaxBulkSize.Value, res.Settings.IndexSettings.MaxBulkSize.Value)
	require.Equal(t, dbNullableSettings.IndexSettings.BulkPreparationTimeout.Value, res.Settings.IndexSettings.BulkPreparationTimeout.Value)

	require.Equal(t, dbNullableSettings.AhtSettings.SyncThreshold.Value, res.Settings.AhtSettings.SyncThreshold.Value)
	require.Equal(t, dbNullableSettings.AhtSettings.WriteBufferSize.Value, res.Settings.AhtSettings.WriteBufferSize.Value)

	require.Equal(t, dbNullableSettings.TruncationSettings.RetentionPeriod.Value, res.Settings.TruncationSettings.RetentionPeriod.Value)
	require.Equal(t, dbNullableSettings.TruncationSettings.TruncationFrequency.Value, res.Settings.TruncationSettings.TruncationFrequency.Value)

	_, err = client.UpdateDatabaseV2(ctx, "db1", &schema.DatabaseNullableSettings{})
	require.NoError(t, err)
}

func TestCreateDatabaseWithUnderscoreCharacter(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	_, err := client.CreateDatabaseV2(ctx, "db_with_", nil)
	require.NoError(t, err)

	_, err = client.UseDatabase(ctx, &schema.Database{DatabaseName: "db_with_"})
	require.NoError(t, err)
}
