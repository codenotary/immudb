/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package sessions

import (
	"context"
	stdos "os"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestNewSession(t *testing.T) {
	sess := NewSession("sessID", &auth.User{}, nil, logger.NewSimpleLogger("test", stdos.Stdout))
	require.NotNil(t, sess)
	require.Less(t, sess.GetCreationTime(), time.Now())
	require.Less(t, sess.GetLastActivityTime(), time.Now())
}

func TestGetSessionIDFromContext(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("sessionid", "sessionID"))
	sessionID, err := GetSessionIDFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, sessionID, "sessionID")
	_, err = GetSessionIDFromContext(metadata.NewIncomingContext(ctx, metadata.Pairs("sessionid", "")))
	require.ErrorIs(t, ErrNoSessionIDPresent, err)
	_, err = GetSessionIDFromContext(context.Background())
	require.ErrorIs(t, ErrNoSessionIDPresent, err)
	_, err = GetSessionIDFromContext(metadata.NewIncomingContext(ctx, metadata.Pairs()))
	require.ErrorIs(t, ErrNoSessionAuthDataProvided, err)
}
