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

package logger

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNewLogger(t *testing.T) {
	type args struct {
		opts *Options
	}
	tests := []struct {
		name           string
		args           args
		wantLoggerType Logger
		wantErr        bool
	}{
		{
			name: "with json logger",
			args: args{
				opts: &Options{
					Name:      "foo",
					LogFormat: "json",
				},
			},
			wantLoggerType: &JsonLogger{},
			wantErr:        false,
		},
		{
			name: "with text logger",
			args: args{
				opts: &Options{
					Name:      "foo",
					LogFormat: LogFormatText,
				},
			},
			wantLoggerType: &SimpleLogger{},
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLogger, err := NewLogger(tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLogger() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer gotLogger.Close()
			if reflect.TypeOf(gotLogger) != reflect.TypeOf(tt.wantLoggerType) {
				t.Errorf("NewLogger() = %v, want %v", gotLogger, tt.wantLoggerType)
			}
		})
	}
}

func TestNewLoggerWithFile(t *testing.T) {
	tests := []struct {
		name           string
		opts           *Options
		wantLoggerType Logger
		wantErr        bool
	}{
		{
			name: "with json logger",
			opts: &Options{
				Name:      "foo",
				LogFormat: "json",
				LogFile:   filepath.Join(t.TempDir(), "log_json.log"),
			},
			wantLoggerType: &JsonLogger{},
			wantErr:        false,
		},
		{
			name: "with text logger",
			opts: &Options{
				Name:      "foo",
				LogFormat: LogFormatText,
				LogFile:   filepath.Join(t.TempDir(), "log_text.log"),
			},
			wantLoggerType: &SimpleLogger{},
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLogger, err := NewLogger(tt.opts)
			defer os.RemoveAll(tt.opts.LogFile)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLogger() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer gotLogger.Close()
			if reflect.TypeOf(gotLogger) != reflect.TypeOf(tt.wantLoggerType) {
				t.Errorf("NewLogger() = %v, want %v", gotLogger, tt.wantLoggerType)
			}
		})
	}
}
