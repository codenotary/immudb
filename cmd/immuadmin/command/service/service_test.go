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
	"bytes"
	"github.com/codenotary/immudb/cmd/helper"
	"io/ioutil"
	"testing"

	"github.com/codenotary/immudb/cmd/immuadmin/command/service/servicetest"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestCommandLine_ServiceImmudbInstall(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}

	cld.Service(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"service", "immudb", "install"})
	err := cmd.Execute()
	assert.Nil(t, err)
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "installing")
}

func TestCommandLine_ServiceImmugwInstall(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"y"}

	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}

	cld.Service(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"service", "immugw", "install"})
	err := cmd.Execute()
	assert.Nil(t, err)
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "installing")
}

func TestCommandLine_ServiceImmudbUninstallAbortUnintall(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"n"}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}

	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "immudb", "uninstall"})
	err := cmd.Execute()
	assert.Nil(t, err)
}

func TestCommandLine_ServiceImmudbUninstallRemovingData(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"y", "y"}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}

	cld.Service(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"service", "immudb", "uninstall"})
	err := cmd.Execute()
	assert.Nil(t, err)
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "uninstall")
}

func TestCommandLine_ServiceImmudbUninstallWithoutRemoveData(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"y", "n"}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}

	cld.Service(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"service", "immudb", "uninstall"})
	err := cmd.Execute()
	assert.Nil(t, err)
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "uninstall")
}

func TestCommandLine_ServiceImmugwUninstall(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	tr.Responses = []string{"y"}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}

	cld.Service(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"service", "immugw", "uninstall"})
	err := cmd.Execute()
	assert.Nil(t, err)
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "uninstall")
}

func TestCommandLine_ServiceImmudbStop(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "immudb", "stop"})
	err := cmd.Execute()
	assert.Nil(t, err)
}

func TestCommandLine_ServiceImmudbStart(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "immudb", "start"})
	err := cmd.Execute()
	assert.Nil(t, err)
}

func TestCommandLine_ServiceImmugwStop(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "immugw", "stop"})
	err := cmd.Execute()
	assert.Nil(t, err)
}

func TestCommandLine_ServiceImmugwStart(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "immugw", "start"})
	err := cmd.Execute()
	assert.Nil(t, err)
}

func TestCommandLine_ServiceImmudbDelayed(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "immudb", "stop", "--time", "20"})
	err := cmd.Execute()
	assert.Nil(t, err)
}

func TestCommandLine_ServiceImmugwDelayed(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "immugw", "stop", "--time", "20"})
	err := cmd.Execute()
	assert.Nil(t, err)
}

func TestCommandLine_ServiceImmudbRestart(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "immudb", "restart"})
	err := cmd.Execute()
	assert.Nil(t, err)
}

func TestCommandLine_ServiceImmugwRestart(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "immugw", "restart"})
	err := cmd.Execute()
	assert.Nil(t, err)
}

func TestCommandLine_ServiceImmudbStatus(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "immudb", "status"})
	err := cmd.Execute()
	assert.Nil(t, err)
}

func TestCommandLine_ServiceImmugwStatus(t *testing.T) {
	cmd := &cobra.Command{}
	tr := &clienttest.TerminalReaderMock{}
	cld := commandline{helper.Config{}, servicetest.Sservicemock{}, tr}
	cld.Service(cmd)
	cmd.SetArgs([]string{"service", "immugw", "status"})
	err := cmd.Execute()
	assert.Nil(t, err)
}
