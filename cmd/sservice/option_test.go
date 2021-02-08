/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package sservice

import (
	"bytes"
	"testing"
)

func TestOption(t *testing.T) {
	op := &Option{}
	op = op.WithConfigPath("/configpath").
		WithExecPath("/execpath").
		WithGroup("groupname").
		WithUsageDetails("service usage details").
		WithUsageExamples("service usage examples").
		WithUser("/user").
		WithStartUpConfig("startupconfig").
		WithConfig([]byte("immuclientconfig"))
	if (op.ConfigPath != "/configpath") ||
		(op.ExecPath != "/execpath") ||
		(op.Group != "groupname") ||
		(op.UsageDetails != "service usage details") ||
		(op.UsageExamples != "service usage examples") ||
		(op.User != "/user") ||
		(op.StartUpConfig != "startupconfig") ||
		(!bytes.Equal(op.Config, []byte("immuclientconfig"))) {
		t.Fatal("service option fail")
	}
}
