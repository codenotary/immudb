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

package immuc

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/status"
)

func (i *immuc) Login(args []string) (CommandOutput, error) {
	var user []byte
	if len(args) >= 1 {
		user = []byte(args[0])
	} else if len(i.options.immudbClientOptions.Username) > 0 {
		user = []byte(i.options.immudbClientOptions.Username)
	} else {
		return nil, errors.New("please specify a username")
	}

	var pass []byte
	var err error
	if len(i.options.immudbClientOptions.Password) == 0 {
		pass, err = i.options.immudbClientOptions.PasswordReader.Read("Password:")
		if err != nil {
			return nil, err
		}
	} else {
		pass = []byte(i.options.immudbClientOptions.Password)
	}

	ctx := context.Background()
	response, err := i.ImmuClient.Login(ctx, user, pass)
	if err != nil {
		if strings.Contains(err.Error(), "authentication disabled") {
			return nil, errors.New("authentication is disabled on server")
		}
		return nil, err
	}

	i.isLoggedin = true

	return &resultOutput{
		Result:  "Successfully logged in",
		Warning: string(response.Warning),
	}, nil
}

func (i *immuc) Logout(args []string) (CommandOutput, error) {
	var err error
	i.isLoggedin = false
	err = i.ImmuClient.Logout(context.TODO())
	st, ok := status.FromError(err)
	if ok && st.Message() == "not logged in" {
		return &errorOutput{err: "User not logged in"}, nil
	}
	if err != nil {
		return nil, err
	}
	return &resultOutput{Result: "Successfully logged out"}, nil
}

func (i *immuc) UseDatabase(args []string) (CommandOutput, error) {
	var dbname string
	if len(args) > 0 {
		dbname = args[0]
	} else if len(i.options.immudbClientOptions.Database) > 0 {
		dbname = i.options.immudbClientOptions.Database
	} else {
		return nil, fmt.Errorf("database name not specified")
	}

	ctx := context.Background()
	_, err := i.execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.UseDatabase(ctx, &schema.Database{
			DatabaseName: dbname,
		})
	})
	if err != nil {
		return nil, err
	}

	i.ImmuClient.GetOptions().CurrentDatabase = dbname

	return &resultOutput{
		Result: fmt.Sprintf("Now using %s", dbname),
	}, nil
}
