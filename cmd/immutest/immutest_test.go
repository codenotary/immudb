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

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

type homedirServiceMock struct {
	content []byte
}

func (hsm *homedirServiceMock) WriteFileToUserHomeDir(content []byte, pathToFile string) error {
	hsm.content = content
	return nil
}
func (hsm *homedirServiceMock) FileExistsInUserHomeDir(pathToFile string) (bool, error) {
	return len(hsm.content) > 0, nil
}
func (hsm *homedirServiceMock) ReadFileFromUserHomeDir(pathToFile string) (string, error) {
	return string(hsm.content), nil
}
func (hsm *homedirServiceMock) DeleteFileFromUserHomeDir(pathToFile string) error {
	hsm.content = nil
	return nil
}

type pwrMock struct {
	readF func(msg string) ([]byte, error)
}

func (pr *pwrMock) Read(msg string) ([]byte, error) {
	return pr.readF(msg)
}

type trMock struct{}

func (tr *trMock) ReadFromTerminalYN(def string) (selected string, err error) {
	return "Y", nil
}

func TestImmutest(t *testing.T) {
	viper.Set("database", "defaultdb")
	viper.Set("user", "immudb")
	data := map[string]string{}
	var index uint64
	icm := &clienttest.ImmuClientMock{
		GetOptionsF: func() *client.Options {
			return client.DefaultOptions()
		},
		LoginF: func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
			return &schema.LoginResponse{Token: []byte("token")}, nil
		},
		DisconnectF: func() error { return nil },
		UseDatabaseF: func(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error) {
			return &schema.UseDatabaseReply{Token: "token"}, nil
		},
		SetF: func(ctx context.Context, key []byte, value []byte) (*schema.Index, error) {
			data[string(key)] = string(value)
			r := schema.Index{Index: index}
			index++
			return &r, nil
		},
	}

	pwrMock := pwrMock{
		readF: func(string) ([]byte, error) { return []byte("password"), nil },
	}

	execute(
		func(opts *client.Options) (client.ImmuClient, error) {
			return icm, nil
		},
		&pwrMock,
		&trMock{},
		&homedirServiceMock{},
		func(err error) {
			require.NoError(t, err)
		},
		[]string{"3"})
	require.Equal(t, 3, len(data))

	icErr := errors.New("some immuclient error")
	execute(
		func(opts *client.Options) (client.ImmuClient, error) {
			return nil, icErr
		},
		&pwrMock,
		&trMock{},
		&homedirServiceMock{},
		func(err error) {
			require.Error(t, err)
			require.Equal(t, icErr, err)
		},
		[]string{"3"})
}
