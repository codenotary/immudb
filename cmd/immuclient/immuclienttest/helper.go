/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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

	"github.com/codenotary/immudb/pkg/client/tokenservice"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientTest struct {
	Imc     immuc.Client
	Ts      tokenservice.TokenService
	Options immuc.Options
	Pr      helper.PasswordReader
	Dialer  servertest.BuffDialer
}

type HomedirServiceMock struct {
	m sync.RWMutex
	f map[string][]byte
}

func (h *HomedirServiceMock) FileExistsInUserHomeDir(pathToFile string) (bool, error) {
	h.m.RLock()
	defer h.m.RUnlock()
	if h.f == nil {
		return false, nil
	}
	_, exists := h.f[pathToFile]
	return exists, nil
}

func (h *HomedirServiceMock) WriteFileToUserHomeDir(content []byte, pathToFile string) error {
	h.m.Lock()
	defer h.m.Unlock()
	if h.f == nil {
		h.f = map[string][]byte{}
	}
	h.f[pathToFile] = content
	return nil
}

func (h *HomedirServiceMock) DeleteFileFromUserHomeDir(pathToFile string) error {
	h.m.Lock()
	defer h.m.Unlock()
	if h.f != nil {
		delete(h.f, pathToFile)
	}
	return nil
}

func (h *HomedirServiceMock) ReadFileFromUserHomeDir(pathToFile string) (string, error) {
	h.m.RLock()
	defer h.m.RUnlock()
	if h.f == nil {
		return "", os.ErrNotExist
	}
	c, exists := h.f[pathToFile]
	if !exists {
		return "", os.ErrNotExist
	}
	return string(c), nil
}

func NewDefaultClientTest() *ClientTest {
	return &ClientTest{}
}
func NewClientTest(pr helper.PasswordReader, tkns tokenservice.TokenService, opts *client.Options) *ClientTest {
	return &ClientTest{
		Ts:      tkns,
		Pr:      pr,
		Options: *(&immuc.Options{}).WithImmudbClientOptions(opts),
	}
}

func (ct *ClientTest) WithTokenFileService(tkns tokenservice.TokenService) *ClientTest {
	ct.Imc.WithFileTokenService(tkns)
	return ct
}

func (ct *ClientTest) WithOptions(opts *immuc.Options) *ClientTest {
	ct.Options = *opts
	return ct
}

func (c *ClientTest) Connect(dialer servertest.BuffDialer) {

	c.Options.WithRevisionSeparator("@")
	c.Options.GetImmudbClientOptions().
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
		}).
		WithPasswordReader(c.Pr)
	c.Dialer = dialer

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

func (c *ClientTest) Login(username string) {
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
