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
	"log"
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type homedirServiceMock struct {
	client.HomedirService
	token []byte
}

func (h *homedirServiceMock) FileExistsInUserHomeDir(pathToFile string) (bool, error) {
	return true, nil
}

func (h *homedirServiceMock) WriteFileToUserHomeDir(content []byte, pathToFile string) error {
	h.token = content
	return nil
}

func (h *homedirServiceMock) DeleteFileFromUserHomeDir(pathToFile string) error {
	return nil
}

func (h *homedirServiceMock) ReadFileFromUserHomeDir(pathToFile string) (string, error) {
	return string(h.token), nil
}

func newClient(pr helper.PasswordReader, dialer servertest.BuffDialer, hds client.HomedirService) immuc.Client {
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(dialer), grpc.WithInsecure(),
	}

	var ic immuc.Client
	var err error
	if hds == nil {
		ic, err = immuc.Init(immuc.Options().WithDialOptions(&dialOptions).WithPasswordReader(pr).
			WithHomedirService(&homedirServiceMock{}))
	} else {
		ic, err = immuc.Init(immuc.Options().WithDialOptions(&dialOptions).WithPasswordReader(pr).
			WithHomedirService(hds))
	}

	err = ic.Connect([]string{""})
	if err != nil {
		log.Fatal(err)
	}
	return ic
}

func login(dialer servertest.BuffDialer) (immuc.Client, client.HomedirService) {
	viper.Set("tokenfile", client.DefaultOptions().TokenFileName)
	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb"},
	}
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(dialer), grpc.WithInsecure(),
	}
	hds := &homedirServiceMock{}
	ic, err := immuc.Init(immuc.Options().WithDialOptions(&dialOptions).WithPasswordReader(pr).
		WithHomedirService(hds))

	err = ic.Connect([]string{""})
	if err != nil {
		log.Fatal(err)
	}
	_, err = ic.Login([]string{"immudb"})
	if err != nil {
		log.Fatal(err)
	}
	return ic, hds
}

func TestInit(t *testing.T) {
	opts := helper.Options{}
	cm := Init(&opts)
	if len(cm.Commands()) != 29 {
		t.Fatal("fail immuclient commands, wrong number of expected commanfs")
	}
}

func TestConnect(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	cmd := commandline{
		immucl: newClient(&immuclienttest.PasswordReader{}, bs.Dialer, nil),
	}
	_ = cmd.connect(&cobra.Command{}, []string{})
	cmd.disconnect(&cobra.Command{}, []string{})
}
