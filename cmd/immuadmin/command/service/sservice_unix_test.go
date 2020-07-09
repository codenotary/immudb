// +build linux

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

package service

import (
	"github.com/codenotary/immudb/cmd/immuadmin/command/service/servicetest"
	"github.com/stretchr/testify/assert"
	daem "github.com/takama/daemon"
	"testing"
)

func TestSservice_NewDaemon(t *testing.T) {
	ss := sservice{servicetest.Ossmock{}, &servicetest.ConfigServiceMock{}}
	d, err := ss.NewDaemon("test", "", "")
	assert.Nil(t, err)
	dc, _ := daem.New("test", "", "")
	assert.IsType(t, d, dc)
}

func TestSservice_IsAdmin(t *testing.T) {
	ss := sservice{servicetest.Ossmock{}, &servicetest.ConfigServiceMock{}}
	_, err := ss.IsAdmin()
	assert.Errorf(t, err, "you must have root user privileges. Possibly using 'sudo' command should help")
}
