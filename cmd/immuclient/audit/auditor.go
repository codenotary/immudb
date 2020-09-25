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
	"github.com/codenotary/immudb/pkg/client/rootservice"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/immuos"
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

// ErrAgentNotActive ...
var ErrAgentNotActive = errors.New("agent not active")

func (cAgent *auditAgent) InitAgent() (AuditAgent, error) {
	var err error
	if cAgent.immuc, err = client.NewImmuClient(cAgent.opts); err != nil || cAgent.immuc == nil {
		return nil, fmt.Errorf("Initialization failed: %s \n", err.Error())
	}
	ctx := context.Background()
	sclient := cAgent.immuc.GetServiceClient()
	cAgent.uuidProvider = rootservice.NewImmudbUuidProvider(*sclient)
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

	serverID, err := cAgent.uuidProvider.CurrentUuid(ctx)
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
	auditSignature := viper.GetString("audit-signature")
	if err != nil {
		return nil, err
	}
	if len(auditUsername) > 0 || len(auditPassword) > 0 {
		if _, err = cAgent.immuc.Login(ctx, []byte(auditUsername), []byte(auditPassword)); err != nil {
			return nil, fmt.Errorf("Invalid login operation: %v", err)
		}
	}
	cAgent.ImmuAudit, err = auditor.DefaultAuditor(time.Duration(cAgent.cycleFrequency)*time.Second,
		fmt.Sprintf("%s:%v", options().Address, options().Port),
		cliOpts.DialOptions,
		auditUsername,
		auditPassword,
		auditSignature,
		*cAgent.immuc.GetServiceClient(),
		cAgent.uuidProvider,
		cache.NewHistoryFileCache(filepath.Join(os.TempDir(), "auditor")),
		cAgent.metrics.updateMetrics, cAgent.logger)
	if err != nil {
		return nil, err
	}
	return cAgent, nil
}
