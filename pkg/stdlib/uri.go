/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package stdlib

import (
	"crypto/tls"
	"errors"
	"net/url"
	"strconv"
	"strings"

	"github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func ParseConfig(uri string) (*client.Options, error) {
	if strings.HasPrefix(uri, "immudb://") {
		url, err := url.Parse(uri)
		if err != nil {
			return nil, ErrBadQueryString
		}

		pw, _ := url.User.Password()
		port, _ := strconv.Atoi(url.Port())

		sslMode := url.Query().Get("sslmode")
		dialOptions, err := dialOptions(sslMode)
		if err != nil {
			return nil, err
		}

		cliOpts := client.DefaultOptions().
			WithUsername(url.User.Username()).
			WithPassword(pw).
			WithPort(port).
			WithAddress(url.Hostname()).
			WithDatabase(url.Path[1:]).
			WithDialOptions(dialOptions)

		return cliOpts, nil
	}

	return nil, ErrBadQueryString
}

func GetUri(o *client.Options) string {
	u := url.URL{
		Scheme: "immudb",
		User: url.UserPassword(
			o.Username,
			o.Password,
		),
		Host: strings.Join([]string{o.Address, ":", strconv.Itoa(o.Port)}, ""),
		Path: o.Database,
	}

	return u.String()
}

func dialOptions(sslmode string) ([]grpc.DialOption, error) {
	if sslmode == "" {
		sslmode = "disable"
	}

	switch sslmode {
	case "disable":
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	case "insecure-verify":
		return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}))}, nil
	case "require":
		return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{}))}, nil
	default:
		return nil, errors.New("sslmode is invalid")
	}
}
