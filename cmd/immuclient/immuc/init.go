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

package immuc

import (
	"errors"
	"fmt"
	"strings"

	"github.com/codenotary/immudb/pkg/client/tokenservice"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/viper"
	"google.golang.org/grpc/status"
)

type immuc struct {
	ImmuClient client.ImmuClient
	options    *Options
	isLoggedin bool
}

// Client ...
type Client interface {
	Connect(args []string) error
	Disconnect(args []string) error
	Execute(f func(immuClient client.ImmuClient) (interface{}, error)) (interface{}, error)
	ServerInfo(args []string) (string, error)
	HealthCheck(args []string) (string, error)
	DatabaseHealth(args []string) (string, error)
	CurrentState(args []string) (string, error)
	GetTxByID(args []string) (string, error)
	VerifiedGetTxByID(args []string) (string, error)
	Get(args []string) (string, error)
	VerifiedGet(args []string) (string, error)
	Login(args []string) (string, error)
	Logout(args []string) (string, error)
	History(args []string) (string, error)
	SetReference(args []string) (string, error)
	VerifiedSetReference(args []string) (string, error)
	ZScan(args []string) (string, error)
	Scan(args []string) (string, error)
	Count(args []string) (string, error)
	Set(args []string) (string, error)
	Restore(args []string) (string, error)
	VerifiedSet(args []string) (string, error)
	DeleteKey(args []string) (string, error)
	ZAdd(args []string) (string, error)
	VerifiedZAdd(args []string) (string, error)
	CreateDatabase(args []string) (string, error)
	DatabaseList(args []string) (string, error)
	UseDatabase(args []string) (string, error)
	UserCreate(args []string) (string, error)
	SetActiveUser(args []string, active bool) (string, error)
	SetUserPermission(args []string) (string, error)
	UserList(args []string) (string, error)
	ChangeUserPassword(args []string) (string, error)
	ValueOnly() bool     // TODO: ?
	SetValueOnly(v bool) // TODO: ?
	SQLExec(args []string) (string, error)
	SQLQuery(args []string) (string, error)
	ListTables() (string, error)
	DescribeTable(args []string) (string, error)

	WithFileTokenService(tkns tokenservice.TokenService) Client
}

// Init ...
func Init(opts *Options) (*immuc, error) {
	ic := new(immuc)
	ic.options = opts
	return ic, nil
}

func (i *immuc) Connect(args []string) (err error) {
	if i.ImmuClient, err = client.NewImmuClient(i.options.immudbClientOptions); err != nil {
		return err
	}
	i.WithFileTokenService(tokenservice.NewFileTokenService())
	i.options.immudbClientOptions.Auth = true

	return nil
}

func (i *immuc) Disconnect(args []string) error {
	if err := i.ImmuClient.Disconnect(); err != nil {
		return err
	}
	return nil
}

func (i *immuc) Execute(f func(immuClient client.ImmuClient) (interface{}, error)) (interface{}, error) {
	r, err := f(i.ImmuClient)
	if err == nil {
		return r, nil
	}

	needsLogin := strings.Contains(err.Error(), "token has expired") ||
		strings.Contains(err.Error(), "not logged in") ||
		strings.Contains(err.Error(), "please select a database first")
	if !needsLogin ||
		len(i.ImmuClient.GetOptions().Username) == 0 ||
		len(i.ImmuClient.GetOptions().Password) == 0 {
		return nil, err
	}

	_, err = i.Login(nil)
	if err != nil {
		return nil, fmt.Errorf("error during automatic (re)login: %v", err)
	}
	if len(i.options.immudbClientOptions.Database) > 0 {
		if _, err := i.UseDatabase(nil); err != nil {
			gRPCStatus, ok := status.FromError(err)
			if ok {
				err = errors.New(gRPCStatus.Message())
			}
			return nil, fmt.Errorf(
				"error using database %s after automatic (re)login: %v", i.options.immudbClientOptions.Database, err)
		}
	}

	return f(i.ImmuClient)
}

func (i *immuc) ValueOnly() bool {
	return i.options.valueOnly
}

func (i *immuc) SetValueOnly(v bool) {
	i.options.WithValueOnly(v)
}

func (i *immuc) WithFileTokenService(tkns tokenservice.TokenService) Client {
	if i.ImmuClient != nil {
		i.ImmuClient.WithTokenService(tkns)
	}
	return i
}

func OptionsFromEnv() *Options {
	password, _ := auth.DecodeBase64Password(viper.GetString("password"))
	immudbOptions := client.DefaultOptions().
		WithPort(viper.GetInt("immudb-port")).
		WithAddress(viper.GetString("immudb-address")).
		WithUsername(viper.GetString("username")).
		WithPassword(password).
		WithDatabase(viper.GetString("database")).
		WithTokenFileName(viper.GetString("tokenfile")).
		WithMTLs(viper.GetBool("mtls")).
		WithServerSigningPubKey(viper.GetString("server-signing-pub-key"))

	if viper.GetBool("mtls") {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		immudbOptions.WithMTLsOptions(
			client.DefaultMTLsOptions().
				WithServername(viper.GetString("servername")).
				WithCertificate(viper.GetString("certificate")).
				WithPkey(viper.GetString("pkey")).
				WithClientCAs(viper.GetString("clientcas")),
		)
	}

	opts := (&Options{}).
		WithImmudbClientOptions(immudbOptions).
		WithValueOnly(viper.GetBool("value-only")).
		WithRevisionSeparator(viper.GetString("revision-separator"))

	return opts
}
