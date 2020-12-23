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

package immuclient

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	cmd := NewCommand()
	if len(cmd.Commands()) != 24 {
		t.Fatalf("error initialising command expected %d, got %d", 28, len(cmd.Commands()))
	}
	cmd.SetArgs([]string{"--help"})

	err := Execute(cmd)
	require.NoError(t, err)
}
