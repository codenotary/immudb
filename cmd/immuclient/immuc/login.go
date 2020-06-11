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

package immuc

import (
	"context"
	"errors"
	"strings"

	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) Login(args []string) (string, error) {
	user := []byte(args[0])
	pass, err := i.passwordReader.Read("Password:")
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	response, err := i.ImmuClient.Login(ctx, user, pass)
	if err != nil {
		if strings.Contains(err.Error(), "authentication is disabled on server") {
			return "authentication is disabled on server", nil
		}
		return "", errors.New("username or password is not valid")
	}
	tokenFileName := i.ImmuClient.GetOptions().TokenFileName
	if err = client.WriteFileToUserHomeDir(response.Token, tokenFileName); err != nil {
		return "", err
	}
	i.ImmuClient.GetOptions().Auth = true
	i.ImmuClient, err = client.NewImmuClient((i.ImmuClient.GetOptions()))
	if err != nil {
		return "", err
	}
	i.isLoggedin = true

	return "Successfully logged in", nil
}

func (i *immuc) Logout(args []string) (string, error) {
	len, err := client.ReadFileFromUserHomeDir(i.ImmuClient.GetOptions().TokenFileName)
	if err != nil || len == "" {
		return "User not logged in.", nil
	}
	client.DeleteFileFromUserHomeDir(i.ImmuClient.GetOptions().TokenFileName)
	i.isLoggedin = false
	i.ImmuClient.GetOptions().Auth = false

	i.ImmuClient, err = client.NewImmuClient((i.ImmuClient.GetOptions()))
	if err != nil {
		return "", err
	}
	return "Successfully logged out", nil
}
