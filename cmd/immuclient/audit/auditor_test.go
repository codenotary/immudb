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

package audit

/*
import (
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/embedded/logger"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestInitAgent(t *testing.T) {
	defer viper.Reset()

	srvoptions := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(srvoptions)
	bs.Start()
defer bs.Stop()

	os.Setenv("audit-agent-interval", "1s")
	pidPath := "pid_path"
	os.RemoveAll(pidPath)
	defer os.RemoveAll(pidPath)
	viper.Set("pidfile", pidPath)

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ad := new(auditAgent)
	ad.logger = logger.NewSimpleLogger("TestInitAgent", os.Stderr)
	ad.opts = options().WithMetrics(false).WithDialOptions(dialOptions).WithMTLs(false)
	_, err := ad.InitAgent()
	os.RemoveAll(pidPath)
	require.NoError(t, err, "InitAgent")

	os.Setenv("audit-agent-interval", "X")
	_, err = ad.InitAgent()
	os.RemoveAll(pidPath)
	require.ErrorContains(t, err, "invalid duration")
	os.Unsetenv("audit-agent-interval")

	auditPassword := viper.GetString("audit-password")
	viper.Set("audit-password", "X")
	_, err = ad.InitAgent()
	os.RemoveAll(pidPath)
	require.ErrorContains(t, err, "Invalid login operation")
	viper.Set("audit-password", auditPassword)
}
*/
