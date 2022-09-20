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

package server

import (
	"testing"
	"time"
)

func Test_dbOptions_validateReplicationOptions(t *testing.T) {
	type fields struct {
		Database                string
		synced                  bool
		SyncFrequency           Milliseconds
		Replica                 bool
		MasterDatabase          string
		MasterAddress           string
		MasterPort              int
		FollowerUsername        string
		FollowerPassword        string
		SyncFollowers           int
		FileSize                int
		MaxKeyLen               int
		MaxValueLen             int
		MaxTxEntries            int
		ExcludeCommitTime       bool
		MaxConcurrency          int
		MaxIOConcurrency        int
		WriteBufferSize         int
		TxLogCacheSize          int
		VLogMaxOpenedFiles      int
		TxLogMaxOpenedFiles     int
		CommitLogMaxOpenedFiles int
		WriteTxHeaderVersion    int
		ReadTxPoolSize          int
		IndexOptions            *indexOptions
		AHTOptions              *ahtOptions
		Autoload                featureState
		CreatedBy               string
		CreatedAt               time.Time
		UpdatedBy               string
		UpdatedAt               time.Time
		SyncReplication         bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid non replica",
			fields: fields{
				Replica:         false,
				SyncFollowers:   3,
				SyncReplication: true,
			},
			wantErr: false,
		},
		{
			name: "valid replication options",
			fields: fields{
				Replica:        true,
				SyncFollowers:  0,
				MasterPort:     1,
				MasterDatabase: "foo",
				MasterAddress:  "127.0.0.1",
			},
			wantErr: false,
		},
		{
			name: "replica with no followers",
			fields: fields{
				Replica:         false,
				SyncFollowers:   0,
				SyncReplication: true,
			},
			wantErr: true,
		},
		{
			name: "invalid port",
			fields: fields{
				Replica:        true,
				SyncFollowers:  0,
				MasterPort:     0,
				MasterDatabase: "foo",
				MasterAddress:  "127.0.0.1",
			},
			wantErr: true,
		},
		{
			name: "invalid address",
			fields: fields{
				Replica:        true,
				SyncFollowers:  0,
				MasterPort:     1,
				MasterDatabase: "foo",
				MasterAddress:  "127.0.0",
			},
			wantErr: true,
		},
		{
			name: "invalid sync followers",
			fields: fields{
				Replica:        true,
				SyncFollowers:  1,
				MasterPort:     1,
				MasterDatabase: "foo",
				MasterAddress:  "127.0.0.1",
			},
			wantErr: true,
		},
		{
			name: "invalid database",
			fields: fields{
				Replica:        true,
				SyncFollowers:  0,
				MasterPort:     1,
				MasterDatabase: "",
				MasterAddress:  "127.0.0.1",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &dbOptions{
				Database:                tt.fields.Database,
				synced:                  tt.fields.synced,
				SyncFrequency:           tt.fields.SyncFrequency,
				Replica:                 tt.fields.Replica,
				MasterDatabase:          tt.fields.MasterDatabase,
				MasterAddress:           tt.fields.MasterAddress,
				MasterPort:              tt.fields.MasterPort,
				FollowerUsername:        tt.fields.FollowerUsername,
				FollowerPassword:        tt.fields.FollowerPassword,
				SyncFollowers:           tt.fields.SyncFollowers,
				FileSize:                tt.fields.FileSize,
				MaxKeyLen:               tt.fields.MaxKeyLen,
				MaxValueLen:             tt.fields.MaxValueLen,
				MaxTxEntries:            tt.fields.MaxTxEntries,
				ExcludeCommitTime:       tt.fields.ExcludeCommitTime,
				MaxConcurrency:          tt.fields.MaxConcurrency,
				MaxIOConcurrency:        tt.fields.MaxIOConcurrency,
				WriteBufferSize:         tt.fields.WriteBufferSize,
				TxLogCacheSize:          tt.fields.TxLogCacheSize,
				VLogMaxOpenedFiles:      tt.fields.VLogMaxOpenedFiles,
				TxLogMaxOpenedFiles:     tt.fields.TxLogMaxOpenedFiles,
				CommitLogMaxOpenedFiles: tt.fields.CommitLogMaxOpenedFiles,
				WriteTxHeaderVersion:    tt.fields.WriteTxHeaderVersion,
				ReadTxPoolSize:          tt.fields.ReadTxPoolSize,
				IndexOptions:            tt.fields.IndexOptions,
				AHTOptions:              tt.fields.AHTOptions,
				Autoload:                tt.fields.Autoload,
				CreatedBy:               tt.fields.CreatedBy,
				CreatedAt:               tt.fields.CreatedAt,
				UpdatedBy:               tt.fields.UpdatedBy,
				UpdatedAt:               tt.fields.UpdatedAt,
				SyncReplication:         tt.fields.SyncReplication,
			}
			if err := opts.validateReplicationOptions(); (err != nil) != tt.wantErr {
				t.Errorf("dbOptions.validateReplicationOptions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
