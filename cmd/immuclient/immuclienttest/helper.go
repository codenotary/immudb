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

package immuclienttest

import (
	"bytes"
	"io"
	"log"
	"os"
	"sync"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server/servertest"
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

func NewClient(pr helper.PasswordReader, dialer servertest.BuffDialer, hds client.HomedirService) immuc.Client {
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

func Login(username string, password string, dialer servertest.BuffDialer) (immuc.Client, client.HomedirService) {
	viper.Set("tokenfile", client.DefaultOptions().TokenFileName)
	pr := &PasswordReader{
		Pass: []string{password, password},
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
	_, err = ic.Login([]string{username})
	if err != nil {
		log.Fatal(err)
	}
	return ic, hds
}

func CaptureStdout(f func()) string {
	custReader, custWriter, err := os.Pipe()
	if err != nil {
		panic(err)
	}
	origStdout := os.Stdout
	origStderr := os.Stderr
	defer func() {
		os.Stdout = origStdout
		os.Stderr = origStderr
	}()
	os.Stdout, os.Stderr = custWriter, custWriter
	log.SetOutput(custWriter)
	out := make(chan string)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		var buf bytes.Buffer
		wg.Done()
		io.Copy(&buf, custReader)
		out <- buf.String()
	}()
	wg.Wait()
	f()
	custWriter.Close()
	return <-out
}

type PasswordReader struct {
	Pass       []string
	callNumber int
}

func (pr *PasswordReader) Read(msg string) ([]byte, error) {
	if len(pr.Pass) <= pr.callNumber {
		log.Fatal("Application requested the password more times than number of passwords supplied")
	}
	pass := []byte(pr.Pass[pr.callNumber])
	pr.callNumber++
	return pass, nil
}
