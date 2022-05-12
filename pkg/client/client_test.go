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
package client

import (
	"fmt"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestLogErr(t *testing.T) {
	logger := logger.NewSimpleLogger("client_test", os.Stderr)

	require.Nil(t, logErr(logger, "error: %v", nil))

	err := fmt.Errorf("expected error")
	require.Error(t, logErr(logger, "error: %v", err))
}
