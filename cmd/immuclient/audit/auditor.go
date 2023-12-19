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

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/signer"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/codenotary/immudb/pkg/server"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/auditor"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/spf13/viper"
)

const (
	name        = "immuclient"
	description = "immuclient"
)

// ErrAgentNotActive ...
var ErrAgentNotActive = errors.New("agent not active")

func (cAgent *auditAgent) InitAgent() (AuditAgent, error) {
	var err error
	if cAgent.immuc, err = client.NewImmuClient(cAgent.opts); err != nil || cAgent.immuc == nil {
		return nil, fmt.Errorf("Initialization failed: %s \n", err.Error())
	}
	ctx := context.Background()
	sclient := cAgent.immuc.GetServiceClient()
	cAgent.uuidProvider = state.NewUUIDProvider(sclient)
	if cAgent.opts.PidPath != "" {
		if cAgent.Pid, err = server.NewPid(cAgent.opts.PidPath, immuos.NewStandardOS()); err != nil {
			cAgent.logger.Errorf("failed to write pidfile: %s", err)
			return nil, err
		}
	}

	cAgent.cycleFrequency = 60
	if freqstr := os.Getenv("audit-agent-interval"); freqstr != "" {
		d, err := time.ParseDuration(freqstr)
		if err != nil {
			return nil, err
		}
		cAgent.cycleFrequency = int(d.Seconds())
	}

	serverID, err := cAgent.uuidProvider.CurrentUUID(ctx)
	if serverID == "" || err != nil {
		serverID = "unknown"
	}
	if cAgent.opts.Metrics {
		cAgent.metrics.init(serverID, cAgent.opts.Address, strconv.Itoa(cAgent.opts.Port))
	}
	cliOpts := cAgent.immuc.GetOptions()
	ctx = context.Background()
	auditUsername := viper.GetString("audit-username")
	auditPassword, err := auth.DecodeBase64Password(viper.GetString("audit-password"))
	if err != nil {
		return nil, err
	}
	auditDatabasesStr := viper.GetString("audit-databases")
	auditDatabasesArr := strings.Split(auditDatabasesStr, ",")
	var auditDatabases []string
	for _, dbPrefix := range auditDatabasesArr {
		dbPrefix = strings.TrimSpace(dbPrefix)
		if len(dbPrefix) > 0 {
			auditDatabases = append(auditDatabases, dbPrefix)
		}
	}
	auditNotificationURL := viper.GetString("audit-notification-url")
	auditNotificationUsername := viper.GetString("audit-notification-username")
	auditNotificationPassword := viper.GetString("audit-notification-password")
	if len(auditUsername) > 0 || len(auditPassword) > 0 {
		if _, err = cAgent.immuc.Login(ctx, []byte(auditUsername), []byte(auditPassword)); err != nil {
			return nil, fmt.Errorf("Invalid login operation: %v", err)
		}
	}

	auditMonitoringHTTPAddr := fmt.Sprintf(
		"%s:%d",
		viper.GetString("audit-monitoring-host"), viper.GetInt("audit-monitoring-port"))

	var pk *ecdsa.PublicKey
	if cliOpts.ServerSigningPubKey != "" {
		pk, err = signer.ParsePublicKeyFile(cliOpts.ServerSigningPubKey)
		if err != nil {
			return nil, err
		}
	}

	cAgent.ImmuAudit, err = auditor.DefaultAuditor(time.Duration(cAgent.cycleFrequency)*time.Second,
		fmt.Sprintf("%s:%v", options().Address, options().Port),
		cliOpts.DialOptions,
		auditUsername,
		auditPassword,
		auditDatabases,
		pk,
		auditor.AuditNotificationConfig{
			URL:            auditNotificationURL,
			Username:       auditNotificationUsername,
			Password:       auditNotificationPassword,
			RequestTimeout: time.Duration(5) * time.Second,
		},
		cAgent.immuc.GetServiceClient(),
		cAgent.uuidProvider,
		cache.NewHistoryFileCache(filepath.Join(os.TempDir(), "auditor")),
		cAgent.metrics.updateMetrics,
		cAgent.logger,
		&auditMonitoringHTTPAddr)
	if err != nil {
		return nil, err
	}
	return cAgent, nil
}
