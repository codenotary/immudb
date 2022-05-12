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

package sessions

import (
	"context"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	stdos "os"
	"testing"
)

func TestNewSession(t *testing.T) {
	sess := NewSession("sessID", &auth.User{}, nil, logger.NewSimpleLogger("test", stdos.Stdout))
	st := sess.GetStatus()
	require.Equal(t, active, st)
}

func TestGetSessionIDFromContext(t *testing.T) {
	ctx := context.TODO()
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("sessionid", "sessionID"))
	sessionID, err := GetSessionIDFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, sessionID, "sessionID")
	_, err = GetSessionIDFromContext(metadata.NewIncomingContext(ctx, metadata.Pairs("sessionid", "")))
	require.ErrorIs(t, ErrNoSessionIDPresent, err)
	_, err = GetSessionIDFromContext(context.TODO())
	require.ErrorIs(t, ErrNoSessionIDPresent, err)
	_, err = GetSessionIDFromContext(metadata.NewIncomingContext(ctx, metadata.Pairs()))
	require.ErrorIs(t, ErrNoSessionAuthDataProvided, err)
}
