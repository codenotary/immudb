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

package replication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOptions(t *testing.T) {
	opts := &Options{}
	require.False(t, opts.Valid())

	delayer := &expBackoff{
		retryMinDelay: time.Second,
		retryMaxDelay: 2 * time.Minute,
		retryDelayExp: 2,
		retryJitter:   0.1,
	}

	opts.WithMasterDatabase("defaultdb").
		WithMasterAddress("127.0.0.1").
		WithMasterPort(3322).
		WithFollowerUsername("immudbUsr").
		WithFollowerPassword("immdubPwd").
		WithStreamChunkSize(DefaultChunkSize).
		WithDelayer(delayer)

	require.Equal(t, "defaultdb", opts.masterDatabase)
	require.Equal(t, "127.0.0.1", opts.masterAddress)
	require.Equal(t, 3322, opts.masterPort)
	require.Equal(t, "immudbUsr", opts.followerUsername)
	require.Equal(t, "immdubPwd", opts.followerPassword)
	require.Equal(t, DefaultChunkSize, opts.streamChunkSize)
	require.Equal(t, delayer, opts.delayer)

	require.True(t, opts.Valid())

	defaultOpts := DefaultOptions()
	require.NotNil(t, defaultOpts)
	require.True(t, defaultOpts.Valid())
}
