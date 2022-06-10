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

package cli

import (
	"github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/cmd/version"
)

func (cli *cli) history(args []string) (immuc.CommandOutput, error) {
	return cli.immucl.History(args)
}

func (cli *cli) healthCheck(args []string) (immuc.CommandOutput, error) {
	return cli.immucl.HealthCheck(args)
}

type versionOutput struct {
	Version string `json:"version"`
}

var _ immuc.CommandOutput = &versionOutput{}

func (o *versionOutput) Plain() string     { return o.Version }
func (o *versionOutput) ValueOnly() string { return o.Version }
func (o *versionOutput) Json() interface{} { return o }

func (cli *cli) version(args []string) (immuc.CommandOutput, error) {
	return &versionOutput{
		Version: version.VersionStr(),
	}, nil
}
