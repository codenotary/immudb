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

package util

import (
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	var cases = []struct {
		in      string
		out     time.Duration
		wantErr bool
	}{
		{
			in:      "",
			out:     0,
			wantErr: true,
		},
		{
			in:      "0",
			out:     0,
			wantErr: true,
		},
		{
			in:      "0s",
			out:     0,
			wantErr: true,
		},
		{
			in:      "1",
			out:     0,
			wantErr: true,
		},
		{
			in:      "10ms",
			out:     0,
			wantErr: true,
		},
		{
			in:      "-1d",
			out:     0,
			wantErr: true,
		},
		{
			in:      "-1w",
			out:     0,
			wantErr: true,
		},
		{
			in:      "0.5d",
			out:     0,
			wantErr: true,
		},
		{
			in:      "d",
			out:     0,
			wantErr: true,
		},
		{
			in:      "1s",
			out:     0,
			wantErr: true,
		},
		{
			in:      "10m",
			out:     0,
			wantErr: true,
		},
		{
			in:      "1h",
			out:     0,
			wantErr: true,
		},
		{
			in:  "1d",
			out: 1 * 24 * time.Hour,
		},
		{
			in:      "3d10h",
			out:     0,
			wantErr: true,
		},
		{
			in:  "2w",
			out: 2 * 7 * 24 * time.Hour,
		},
		{
			in:  "2y",
			out: 2 * 365 * 24 * time.Hour,
		},
	}

	for _, c := range cases {
		d, err := ParseDuration(c.in)
		if err != nil && !c.wantErr {
			t.Errorf("error on input %q: %s", c.in, err)
		}
		if time.Duration(d) != c.out {
			t.Errorf("expected %v but got %v", c.out, d)
		}
	}
}
