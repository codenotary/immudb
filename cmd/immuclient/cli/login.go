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

package cli

import (
	"context"
	"errors"
	"strings"

	"github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (cli *cli) login(args []string) (string, error) {
	user := []byte(args[0])
	pass, err := cli.passwordReader.Read("Password:")
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	response, err := cli.ImmuClient.Login(ctx, user, pass)
	if err != nil {
		if strings.Contains(err.Error(), "authentication is disabled on server") {
			return "authentication is disabled on server", nil
		}
		return "", err
	}
	tokenFileName := cli.ImmuClient.GetOptions().TokenFileName
	if err := client.WriteFileToUserHomeDir(response.Token, tokenFileName); err != nil {
		return "", err
	}
	cli.ImmuClient.GetOptions().Auth = true
	cli.ImmuClient, err = client.NewImmuClient((cli.ImmuClient.GetOptions()))
	if err != nil {
		return "", err
	}
	cli.isLoggedin = true

	return "Successfully logged in", nil
}

func (cli *cli) logout(args []string) (string, error) {
	var err error
	if err = cli.ImmuClient.Logout(context.Background()); err != nil {
		s, ok := status.FromError(err)
		if ok && s.Code() == codes.Unauthenticated {
			err = errors.New("Unauthenticated, please login")
		}
		return "", err
	}
	cli.isLoggedin = false
	cli.ImmuClient.GetOptions().Auth = false
	cli.ImmuClient, err = client.NewImmuClient((cli.ImmuClient.GetOptions()))
	if err != nil {
		return "", err
	}
	return "Successfully logged out", nil
}
