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
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/cmd/immuclient/service"
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
	srv, err := service.NewDaemon(name, description, name)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	cAgent.Daemon = srv
	freqstr := os.Getenv("audit-agent-interval")
	cAgent.cycleFrequency = 60
	sclient := cAgent.immuc.GetServiceClient()
	serverID, err := client.GetServerUuid(ctx, *sclient)
	if serverID == "" || err != nil {
		serverID = "unknown"
	}
	cAgent.promot.init(serverID)
	if freqstr != "" {
		s := freqstr[len(freqstr)-1]
		switch string(s) {
		case "s":
			freq, err := strconv.Atoi(freqstr[:len(freqstr)-1])
			if err == nil && freq > 60 {
				cAgent.cycleFrequency = freq
			}
		case "m":
			freq, err := strconv.Atoi(freqstr[:len(freqstr)-1])
			if err == nil && freq > 1 {
				cAgent.cycleFrequency = freq * 60
			}
		case "h":
			freq, err := strconv.Atoi(freqstr[:len(freqstr)-1])
			if err == nil && freq >= 1 {
				cAgent.cycleFrequency = (freq * 60 * 60)
			}
		default:
			freq, err := strconv.Atoi(freqstr[:len(freqstr)-1])
			if err == nil && freq >= 1 {
				cAgent.cycleFrequency = freq
			}
		}
	}
	pathstr := viper.GetString("roots-filepath")
	if pathstr == "/tmp/" {
		filename := filepath.Join(os.TempDir(), filename())
		cAgent.rootStorage, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			QuitToStdErr(err.Error())
		}
	} else {
		pathstr = strings.TrimSuffix(pathstr, "/")
		filename := filepath.Join(pathstr, filename())
		cAgent.rootStorage, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			QuitToStdErr(err.Error())
		}
	}
	cliOpts := cAgent.immuc.GetOptions()

	cAgent.ImmuAudit, err = auditor.DefaultAuditor(time.Duration(cAgent.cycleFrequency)*time.Second,
		fmt.Sprintf("%s:%v", options().Address, options().Port),
		cliOpts.DialOptions,
		viper.GetString("audit-username"),
		viper.GetString("audit-password"),
		cache.NewHistoryFileCache(filepath.Join(os.TempDir(), "auditor")),
		cAgent.promot.exporter)
	if err != nil {
		return nil, err
	}
	return cAgent, nil
}

func filename() string {
	t := time.Now().String()[:16]
	t = strings.ReplaceAll(t, " ", "_")
	t = fmt.Sprintf("audit_roots_%s.txt", t)
	return t
}
