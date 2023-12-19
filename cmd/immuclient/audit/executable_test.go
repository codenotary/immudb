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

package audit

/*
import (
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/embedded/logger"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"google.golang.org/grpc"
)

func TestExecutableRun(t *testing.T) {
	srvoptions := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(srvoptions)
	bs.Start()
defer bs.Stop()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	pidpath := "my_pid"
	ad := new(auditAgent)
	ad.opts = options().WithMetrics(false).WithDialOptions(dialOptions).WithMTLs(false).WithPidPath(pidpath)
	ad.logger = logger.NewSimpleLogger("test", os.Stdout)
	_, err := ad.InitAgent()
	require.NoError(t, err, "InitAgent")
	exec := newExecutable(ad)
	go func() {
		time.Sleep(200 * time.Millisecond)
		exec.Stop()
	}()
	exec.Run()
	os.RemoveAll(pidpath)
}
*/
