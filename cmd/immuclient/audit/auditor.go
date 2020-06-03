/*
Copyright 2019-2020 vChain, Inc.

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

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/codenotary/immudb/pkg/server"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/auditor"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/spf13/viper"
)

const (
	name        = "immuclient"
	description = "immuclient"
)

var ErrAgentNotActive = errors.New("agent not active")

func (cAgent *auditAgent) InitAgent() (AuditAgent, error) {
	var err error
	if cAgent.immuc, err = client.NewImmuClient(options()); err != nil || cAgent.immuc == nil {
		return nil, fmt.Errorf("Initialization failed: %s \n", err.Error())
	}
	ctx := context.Background()

	pidPath := viper.GetString("pidfile")
	if pidPath != "" {
		if cAgent.Pid, err = server.NewPid(pidPath); err != nil {
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
	sclient := cAgent.immuc.GetServiceClient()
	serverID, err := client.GetServerUuid(ctx, *sclient)
	if serverID == "" || err != nil {
		serverID = "unknown"
	}
	cAgent.metrics.init(serverID)
	cliOpts := cAgent.immuc.GetOptions()
	ctx = context.Background()
	auditUsername := []byte(viper.GetString("audit-username"))
	auditPassword := []byte(viper.GetString("audit-password"))
	if len(auditUsername) > 0 || len(auditPassword) > 0 {
		if _, err = cAgent.immuc.Login(ctx, auditUsername, auditPassword); err != nil {
			return nil, fmt.Errorf("Invalid login operation: %v", err)
		}
	}

	cAgent.ImmuAudit, err = auditor.DefaultAuditor(time.Duration(cAgent.cycleFrequency)*time.Second,
		fmt.Sprintf("%s:%v", options().Address, options().Port),
		cliOpts.DialOptions,
		viper.GetString("audit-username"),
		viper.GetString("audit-password"),
		cache.NewHistoryFileCache(filepath.Join(os.TempDir(), "auditor")),
		cAgent.metrics.updateMetrics, cAgent.logfile)
	if err != nil {
		return nil, err
	}
	return cAgent, nil
}
