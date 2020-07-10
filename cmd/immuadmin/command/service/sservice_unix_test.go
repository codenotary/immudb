// +build linux darwin

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
	immudb "github.com/codenotary/immudb/cmd/immudb/command"
	"github.com/codenotary/immudb/cmd/immudb/command/immudbcmdtest"
	"github.com/stretchr/testify/assert"
	daem "github.com/takama/daemon"
	"testing"
)

func TestSservice_NewDaemon(t *testing.T) {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudbcmdtest.ManpageServiceMock{}
	mpss[1] = immudbcmdtest.ManpageServiceMock{}

	ss := sservice{servicetest.Ossmock{}, servicetest.Filepathsmock{}, &servicetest.ConfigServiceMock{}, mpss}
	d, err := ss.NewDaemon("test", "", "")
	assert.Nil(t, err)
	dc, _ := daem.New("test", "", "")
	assert.IsType(t, d, dc)
}

func TestSservice_IsAdmin(t *testing.T) {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudbcmdtest.ManpageServiceMock{}
	mpss[1] = immudbcmdtest.ManpageServiceMock{}

	ss := sservice{servicetest.Ossmock{}, servicetest.Filepathsmock{}, &servicetest.ConfigServiceMock{}, mpss}
	_, err := ss.IsAdmin()
	assert.Errorf(t, err, "you must have root user privileges. Possibly using 'sudo' command should help")
}

func TestSservice_InstallSetup_immudb(t *testing.T) {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudbcmdtest.ManpageServiceMock{}
	mpss[1] = immudbcmdtest.ManpageServiceMock{}

	ss := sservice{servicetest.Ossmock{}, servicetest.Filepathsmock{}, &servicetest.ConfigServiceMock{}, mpss}
	err := ss.InstallSetup("immudb")
	if err != nil {
		t.Logf("TestSservice_InstallSetup_immudb: %s", err)
	}
	assert.Nil(t, err)
}

func TestSservice_InstallSetup_immugw(t *testing.T) {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudbcmdtest.ManpageServiceMock{}
	mpss[1] = immudbcmdtest.ManpageServiceMock{}

	ss := sservice{servicetest.Ossmock{}, servicetest.Filepathsmock{}, &servicetest.ConfigServiceMock{}, mpss}
	err := ss.InstallSetup("immugw")
	if err != nil {
		t.Logf("TestSservice_InstallSetup_immugw: %s", err)
	}
	assert.Nil(t, err)
	assert.Nil(t, err)
}

func TestSservice_UninstallSetup_immudb(t *testing.T) {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudbcmdtest.ManpageServiceMock{}
	mpss[1] = immudbcmdtest.ManpageServiceMock{}

	ss := sservice{servicetest.Ossmock{}, servicetest.Filepathsmock{}, &servicetest.ConfigServiceMock{}, mpss}
	err := ss.UninstallSetup("immudb")
	assert.Nil(t, err)
}

func TestSservice_UninstallSetup_immugw(t *testing.T) {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudbcmdtest.ManpageServiceMock{}
	mpss[1] = immudbcmdtest.ManpageServiceMock{}

	ss := sservice{servicetest.Ossmock{}, servicetest.Filepathsmock{}, &servicetest.ConfigServiceMock{}, mpss}
	err := ss.UninstallSetup("immugw")
	assert.Nil(t, err)
}

func TestSservice_RemoveProgramFiles_immudb(t *testing.T) {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudbcmdtest.ManpageServiceMock{}
	mpss[1] = immudbcmdtest.ManpageServiceMock{}

	ss := sservice{servicetest.Ossmock{}, servicetest.Filepathsmock{}, &servicetest.ConfigServiceMock{}, mpss}
	err := ss.RemoveProgramFiles("immudb")
	assert.Nil(t, err)
}

func TestSservice_EraseData_immudb(t *testing.T) {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudbcmdtest.ManpageServiceMock{}
	mpss[1] = immudbcmdtest.ManpageServiceMock{}

	ss := sservice{servicetest.Ossmock{}, servicetest.Filepathsmock{}, &servicetest.ConfigServiceMock{}, mpss}
	err := ss.EraseData("immudb")
	assert.Nil(t, err)
}

func TestSservice_GetExecutable(t *testing.T) {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudbcmdtest.ManpageServiceMock{}
	mpss[1] = immudbcmdtest.ManpageServiceMock{}

	ss := sservice{servicetest.Ossmock{}, servicetest.Filepathsmock{}, &servicetest.ConfigServiceMock{}, mpss}
	_, err := ss.GetExecutable("immudb", "immudb")
	assert.Nil(t, err)
}

func TestSservice_GetExecutable_emptyInput(t *testing.T) {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudbcmdtest.ManpageServiceMock{}
	mpss[1] = immudbcmdtest.ManpageServiceMock{}

	ss := sservice{servicetest.Ossmock{}, servicetest.Filepathsmock{}, &servicetest.ConfigServiceMock{}, mpss}
	_, err := ss.GetExecutable("", "immudb")
	assert.Nil(t, err)
}

func TestSservice_CopyExecInOsDefault(t *testing.T) {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudbcmdtest.ManpageServiceMock{}
	mpss[1] = immudbcmdtest.ManpageServiceMock{}

	ss := sservice{servicetest.Ossmock{}, servicetest.Filepathsmock{}, &servicetest.ConfigServiceMock{}, mpss}
	_, err := ss.CopyExecInOsDefault("immudb")
	assert.Nil(t, err)
}
