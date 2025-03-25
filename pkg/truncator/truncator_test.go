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

package truncator

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
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
	options := database.DefaultOptions().WithDBRootPath(t.TempDir())

	so := options.GetStoreOptions()

	so.WithIndexOptions(so.IndexOpts.WithCompactionThld(2)).
		WithEmbeddedValues(false).
		WithFileSize(6).
		WithVLogCacheSize(0).
		WithSynced(false)
	options.WithStoreOptions(so)

	ctx := context.Background()

	db := makeDbWith(t, "db", options)

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

		c := database.NewVlogTruncator(db, logger.NewMemoryLogger())

		_, err := c.Plan(ctx, getTruncationTime(queryTime, time.Duration(1*time.Hour)))
		require.ErrorIs(t, err, database.ErrRetentionPeriodNotReached)

		hdr, err := c.Plan(ctx, queryTime)
		require.NoError(t, err)
		require.LessOrEqual(t, time.Unix(hdr.Ts, 0), queryTime)

		err = c.TruncateUptoTx(ctx, hdr.Id)
		require.NoError(t, err)

		// TODO: hard to determine the actual transaction up to which the database was truncated.
		for i := uint64(1); i < 5; i++ {
			kv := &schema.KeyValue{
				Key:   []byte(fmt.Sprintf("key_%d", i)),
				Value: []byte(fmt.Sprintf("val_%d", i)),
			}

			_, err := db.Get(ctx, &schema.KeyRequest{Key: kv.Key})
			require.Error(t, err)
		}

		for i := hdr.Id; i <= 20; i++ {
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
}

func TestTruncator(t *testing.T) {
	options := database.DefaultOptions().WithDBRootPath(t.TempDir())

	so := options.GetStoreOptions().
		WithEmbeddedValues(false)

	so.WithIndexOptions(so.IndexOpts.WithCompactionThld(2)).
		WithFileSize(6)
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
	options := database.DefaultOptions().WithDBRootPath(t.TempDir())

	so := options.GetStoreOptions().
		WithEmbeddedValues(false)

	so.WithIndexOptions(so.IndexOpts.WithCompactionThld(2)).WithFileSize(6)

	options.WithStoreOptions(so)

	db := makeDbWith(t, "db", options)
	tr := NewTruncator(db, 0, 10*time.Millisecond, logger.NewSimpleLogger("immudb ", os.Stderr))

	err := tr.Start()
	require.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	err = tr.Stop()
	require.NoError(t, err)
}

func Test_getTruncationTime(t *testing.T) {
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
			if got := getTruncationTime(tt.args.ts, tt.args.retentionPeriod); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetRetentionPeriod() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTruncator_with_retention_period(t *testing.T) {
	options := database.DefaultOptions().WithDBRootPath(t.TempDir())

	so := options.GetStoreOptions().
		WithEmbeddedValues(false)

	so.WithIndexOptions(so.IndexOpts.WithCompactionThld(2)).WithFileSize(6)
	options.WithStoreOptions(so)

	db := makeDbWith(t, "db", options)
	tr := NewTruncator(db, 25*time.Hour, 5*time.Millisecond, logger.NewSimpleLogger("immudb ", os.Stderr))

	err := tr.Start()
	require.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	err = tr.Truncate(context.Background(), time.Duration(25*time.Hour))
	require.ErrorIs(t, err, database.ErrRetentionPeriodNotReached)

	err = tr.Stop()
	require.NoError(t, err)
}

type mockTruncator struct {
	err error
}

func (m *mockTruncator) Plan(context.Context, time.Time) (*schema.TxHeader, error) {
	return nil, m.err
}

// TruncateUptoTx runs truncation against the relevant appendable logs. Must
// be called after result of Plan().
func (m *mockTruncator) TruncateUptoTx(context.Context, uint64) error {
	return m.err
}

func TestTruncator_with_nothing_to_truncate(t *testing.T) {
	options := database.DefaultOptions().WithDBRootPath(t.TempDir())

	so := options.GetStoreOptions().
		WithEmbeddedValues(false)

	so.WithIndexOptions(so.IndexOpts.WithCompactionThld(2)).WithFileSize(6)
	options.WithStoreOptions(so)

	db := makeDbWith(t, "db", options)
	tr := NewTruncator(db, 25*time.Hour, 5*time.Millisecond, logger.NewSimpleLogger("immudb ", os.Stderr))
	tr.truncators = []database.Truncator{&mockTruncator{err: store.ErrTxNotFound}}

	err := tr.Start()
	require.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	err = tr.Truncate(context.Background(), time.Duration(2*time.Hour))
	require.ErrorIs(t, err, store.ErrTxNotFound)

	err = tr.Stop()
	require.NoError(t, err)
}
