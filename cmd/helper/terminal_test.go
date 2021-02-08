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

package helper

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTerminalReader_ReadFromTerminalYN(t *testing.T) {
	tr := NewTerminalReader(strings.NewReader("Y"))
	resp, err := tr.ReadFromTerminalYN("Y")
	assert.Nil(t, err)
	assert.Equal(t, "y", resp)
	tr.r = strings.NewReader("sgdf")
	resp, err = tr.ReadFromTerminalYN("Y")
	assert.Nil(t, err)
	assert.Equal(t, "", resp)
	tr.r = strings.NewReader("N")
	resp, err = tr.ReadFromTerminalYN("Y")
	assert.Nil(t, err)
	assert.Equal(t, "n", resp)
	tr.r = strings.NewReader("")
	resp, err = tr.ReadFromTerminalYN("Y")
	assert.Nil(t, err)
	assert.Equal(t, "y", resp)
}

func TestStdinPasswordReader_Read(t *testing.T) {
	pr := stdinPasswordReader{&terminalReadPwMock{}}
	pw, err := pr.Read("paxword")
	assert.Nil(t, err)
	assert.Equal(t, []byte(`fake`), pw)
}

func TestTerminalReadPw_ReadPassword(t *testing.T) {
	trp := terminalReadPw{}
	_, err := trp.ReadPassword(int(os.Stdin.Fd()))
	assert.Error(t, err)
}

type terminalReadPwMock struct{}

func (trp *terminalReadPwMock) ReadPassword(fd int) ([]byte, error) {
	return []byte(`fake`), nil
}
