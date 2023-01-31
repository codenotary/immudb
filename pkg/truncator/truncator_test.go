/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package truncator

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
)

func makeDbWith(t *testing.T, dbName string, opts *database.Options) database.DB {
	d, err := database.NewDB(dbName, nil, opts, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	t.Cleanup(func() {
		err := d.Close()
		if !t.Failed() {
			require.NoError(t, err)
		}
	})

	return d
}

func TestDatabase_truncate_with_duration(t *testing.T) {
	rootPath := t.TempDir()

	options := database.DefaultOption().WithDBRootPath(rootPath).WithCorruptionChecker(false)
	so := options.GetStoreOptions()
	so.WithIndexOptions(so.IndexOpts.WithCompactionThld(2)).WithFileSize(6)
	so.MaxIOConcurrency = 1
	so.VLogCacheSize = 0
	options.WithStoreOptions(so)

	ctx := context.Background()
	db := makeDbWith(t, "db", options)
	tr := NewTruncator(db, 0, 0, logger.NewSimpleLogger("immudb ", os.Stderr))
	tr.retentionPeriodF = func(ts time.Time, retentionPeriod time.Duration) time.Time {
		return ts.Add(-1 * retentionPeriod)
	}

	t.Run("truncate with duration", func(t *testing.T) {
		var queryTime time.Time
		for i := 1; i <= 20; i++ {
			kv := &schema.KeyValue{
				Key:   []byte(fmt.Sprintf("key_%d", i)),
				Value: []byte(fmt.Sprintf("val_%d", i)),
			}
			_, err := db.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{kv}})
			require.NoError(t, err)
			if i == 10 {
				queryTime = time.Now()
			}
		}

		c := database.NewVlogTruncator(db)
		hdr, err := c.Plan(queryTime)
		require.NoError(t, err)
		require.LessOrEqual(t, time.Unix(hdr.Ts, 0), queryTime)

		dur := time.Since(queryTime)
		err = tr.Truncate(ctx, dur)
		require.NoError(t, err)

		for i := uint64(1); i < hdr.ID-1; i++ {
			kv := &schema.KeyValue{
				Key:   []byte(fmt.Sprintf("key_%d", i)),
				Value: []byte(fmt.Sprintf("val_%d", i)),
			}

			_, err := db.Get(ctx, &schema.KeyRequest{Key: kv.Key})
			require.Error(t, err)
		}

		for i := hdr.ID; i <= 20; i++ {
			kv := &schema.KeyValue{
				Key:   []byte(fmt.Sprintf("key_%d", i)),
				Value: []byte(fmt.Sprintf("val_%d", i)),
			}

			item, err := db.Get(ctx, &schema.KeyRequest{Key: kv.Key})
			require.NoError(t, err)
			require.Equal(t, kv.Key, item.Key)
			require.Equal(t, kv.Value, item.Value)
		}
	})

	t.Run("truncate with retention period in the past", func(t *testing.T) {
		ts := time.Now().Add(-24 * time.Hour)
		dur := time.Since(ts)
		err := tr.Truncate(ctx, dur)
		require.ErrorIs(t, err, database.ErrRetentionPeriodNotReached)
	})

	t.Run("truncate with retention period in the future", func(t *testing.T) {
		ts := time.Now().Add(24 * time.Hour)
		dur := time.Since(ts)
		err := tr.Truncate(ctx, dur)
		require.ErrorIs(t, err, store.ErrTxNotFound)
	})

}

func TestTruncator(t *testing.T) {
	rootPath := t.TempDir()

	options := database.DefaultOption().WithDBRootPath(rootPath).WithCorruptionChecker(false)
	so := options.GetStoreOptions()
	so.WithIndexOptions(so.IndexOpts.WithCompactionThld(2)).WithFileSize(6)
	so.MaxIOConcurrency = 1
	options.WithStoreOptions(so)

	db := makeDbWith(t, "db", options)
	tr := NewTruncator(db, 0, options.TruncationFrequency, logger.NewSimpleLogger("immudb ", os.Stderr))

	err := tr.Stop()
	require.ErrorIs(t, err, ErrTruncatorAlreadyStopped)

	err = tr.Start()
	require.NoError(t, err)

	err = tr.Start()
	require.ErrorIs(t, err, ErrTruncatorAlreadyRunning)

	err = tr.Stop()
	require.NoError(t, err)
}

func TestTruncator_with_truncation_frequency(t *testing.T) {
	rootPath := t.TempDir()

	options := database.DefaultOption().WithDBRootPath(rootPath).WithCorruptionChecker(false)
	so := options.GetStoreOptions()
	so.WithIndexOptions(so.IndexOpts.WithCompactionThld(2)).WithFileSize(6)
	so.MaxIOConcurrency = 1
	options.WithStoreOptions(so)

	db := makeDbWith(t, "db", options)
	tr := NewTruncator(db, 0, 10*time.Millisecond, logger.NewSimpleLogger("immudb ", os.Stderr))

	err := tr.Start()
	require.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	err = tr.Stop()
	require.NoError(t, err)
}

func Test_GetRetentionPeriod(t *testing.T) {
	type args struct {
		ts              time.Time
		retentionPeriod time.Duration
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{
		{
			args: args{
				ts:              time.Date(2020, 1, 1, 10, 20, 30, 40, time.UTC),
				retentionPeriod: 24 * time.Hour,
			},
			want: time.Date(2019, 12, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			args: args{
				ts:              time.Date(2020, 1, 2, 10, 20, 30, 40, time.UTC),
				retentionPeriod: 24 * time.Hour,
			},
			want: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			args: args{
				ts:              time.Date(2020, 1, 1, 11, 20, 30, 40, time.UTC),
				retentionPeriod: 0,
			},
			want: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getRetentionPeriod(tt.args.ts, tt.args.retentionPeriod); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetRetentionPeriod() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTruncator_with_retention_period(t *testing.T) {
	rootPath := t.TempDir()

	options := database.DefaultOption().WithDBRootPath(rootPath).WithCorruptionChecker(false)
	so := options.GetStoreOptions()
	so.WithIndexOptions(so.IndexOpts.WithCompactionThld(2)).WithFileSize(6)
	so.MaxIOConcurrency = 1
	options.WithStoreOptions(so)

	db := makeDbWith(t, "db", options)
	tr := NewTruncator(db, 25*time.Hour, 5*time.Millisecond, logger.NewSimpleLogger("immudb ", os.Stderr))

	err := tr.Start()
	require.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	err = tr.truncate(context.Background(), getRetentionPeriod(time.Now(), 25*time.Hour))
	require.ErrorIs(t, err, database.ErrRetentionPeriodNotReached)

	err = tr.Stop()
	require.NoError(t, err)
}

type mockTruncator struct {
	err error
}

func (m *mockTruncator) Plan(time.Time) (*store.TxHeader, error) {
	return nil, m.err
}

// Truncate runs truncation against the relevant appendable logs. Must
// be called after result of Plan().
func (m *mockTruncator) Truncate(context.Context, uint64) error {
	return m.err
}

func TestTruncator_with_invalid_transaction_id(t *testing.T) {
	rootPath := t.TempDir()

	options := database.DefaultOption().WithDBRootPath(rootPath).WithCorruptionChecker(false)
	so := options.GetStoreOptions()
	so.WithIndexOptions(so.IndexOpts.WithCompactionThld(2)).WithFileSize(6)
	so.MaxIOConcurrency = 1
	options.WithStoreOptions(so)

	db := makeDbWith(t, "db", options)
	tr := NewTruncator(db, 25*time.Hour, 5*time.Millisecond, logger.NewSimpleLogger("immudb ", os.Stderr))
	tr.truncators = make([]database.Truncator, 0)
	tr.truncators = append(tr.truncators, &mockTruncator{err: store.ErrTxNotFound})

	err := tr.Start()
	require.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	err = tr.truncate(context.Background(), getRetentionPeriod(time.Now(), 2*time.Hour))
	require.ErrorIs(t, err, store.ErrTxNotFound)

	err = tr.Stop()
	require.NoError(t, err)
}
