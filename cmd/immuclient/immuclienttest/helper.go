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

package immuclienttest

import (
	"bytes"
	"io"
	"log"
	"os"
	"sync"

	"github.com/codenotary/immudb/pkg/client/homedir"
	"github.com/codenotary/immudb/pkg/client/tokenservice"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"google.golang.org/grpc"
)

type clientTest struct {
	Imc     immuc.Client
	Ts      tokenservice.TokenService
	Options immuc.Options
	Pr      helper.PasswordReader
}

type HomedirServiceMock struct {
	homedir.HomedirService
	Token []byte
}

func (h *HomedirServiceMock) FileExistsInUserHomeDir(pathToFile string) (bool, error) {
	return true, nil
}

func (h *HomedirServiceMock) WriteFileToUserHomeDir(content []byte, pathToFile string) error {
	h.Token = content
	return nil
}

func (h *HomedirServiceMock) DeleteFileFromUserHomeDir(pathToFile string) error {
	return nil
}

func (h *HomedirServiceMock) ReadFileFromUserHomeDir(pathToFile string) (string, error) {
	return string(h.Token), nil
}

func NewDefaultClientTest() *clientTest {
	return &clientTest{}
}
func NewClientTest(pr helper.PasswordReader, tkns tokenservice.TokenService) *clientTest {
	return &clientTest{
		Ts:      tkns,
		Pr:      pr,
		Options: *(&immuc.Options{}).WithImmudbClientOptions(client.DefaultOptions()),
	}
}

func (ct *clientTest) WithTokenFileService(tkns tokenservice.TokenService) *clientTest {
	ct.Imc.WithFileTokenService(tkns)
	return ct
}

func (ct *clientTest) WithOptions(opts *immuc.Options) *clientTest {
	ct.Options = *opts
	return ct
}

func (c *clientTest) Connect(dialer servertest.BuffDialer) {

	c.Options.WithRevisionSeparator("@")
	c.Options.GetImmudbClientOptions().
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(dialer), grpc.WithInsecure(),
		}).
		WithPasswordReader(c.Pr)

	ic, err := immuc.Init(&c.Options)
	if err != nil {
		log.Fatal(err)
	}
	err = ic.Connect([]string{""})
	if err != nil {
		log.Fatal(err)
	}

	ic.WithFileTokenService(c.Ts)

	c.Imc = ic
}

func (c *clientTest) Login(username string) {
	_, err := c.Imc.Login([]string{username})
	if err != nil {
		log.Fatal(err)
	}
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
