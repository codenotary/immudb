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

package server

import (
	"context"
	"crypto/tls"
	"net/http"
	"strings"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/swagger"
	"github.com/codenotary/immudb/webconsole"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
)

func startWebServer(ctx context.Context, grpcAddr string, httpAddr string, tlsConfig *tls.Config, s *ImmuServer, l logger.Logger) (*http.Server, error) {
	grpcClient, err := grpcClient(ctx, grpcAddr, tlsConfig)
	if err != nil {
		return nil, err
	}

	proxyMux := runtime.NewServeMux(
		runtime.WithIncomingHeaderMatcher(func(key string) (string, bool) {
			switch strings.ToLower(key) {
			case "sessionid":
				return "Sessionid", true
			default:
				return runtime.DefaultHeaderMatcher(key)
			}
		}),
	)

	err = schema.RegisterImmuServiceHandler(ctx, proxyMux, grpcClient)
	if err != nil {
		return nil, err
	}

	err = protomodel.RegisterAuthorizationServiceHandler(ctx, proxyMux, grpcClient)
	if err != nil {
		return nil, err
	}

	err = protomodel.RegisterDocumentServiceHandler(ctx, proxyMux, grpcClient)
	if err != nil {
		return nil, err
	}

	webMux := http.NewServeMux()

	webMux.Handle("/api/", http.StripPrefix("/api", proxyMux))
	webMux.Handle("/api/v2/", http.StripPrefix("/api/v2", proxyMux))

	err = webconsole.SetupWebconsole(webMux, l, httpAddr)
	if err != nil {
		return nil, err
	}

	if s.Options.SwaggerUIEnabled {
		err = swagger.SetupSwaggerUI(webMux, l, httpAddr)
		if err != nil {
			return nil, err
		}
	}

	httpServer := &http.Server{Addr: httpAddr, Handler: webMux}
	httpServer.TLSConfig = tlsConfig

	go func() {
		var err error
		if tlsConfig != nil && len(tlsConfig.Certificates) > 0 {
			l.Infof("web-api server enabled on %s/api (https)", httpAddr)
			err = httpServer.ListenAndServeTLS("", "")
		} else {
			l.Infof("web-api server enabled on %s/api (http)", httpAddr)
			err = httpServer.ListenAndServe()
		}

		if err == http.ErrServerClosed {
			l.Debugf("web-api/console server closed")
		} else {
			l.Errorf("web-api/console error: %s", err)
		}
	}()

	return httpServer, nil
}

func grpcClient(ctx context.Context, grpcAddr string, tlsConfig *tls.Config) (conn *grpc.ClientConn, err error) {
	var creds credentials.TransportCredentials
	if tlsConfig != nil {
		creds = credentials.NewTLS(&tls.Config{RootCAs: tlsConfig.RootCAs})
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err = grpc.Dial(grpcAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return conn, err
	}
	defer func() {
		if err != nil {
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("failed to close conn to %s: %v", grpcAddr, cerr)
			}
			return
		}
		go func() {
			<-ctx.Done()
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("failed to close conn to %s: %v", grpcAddr, cerr)
			}
		}()
	}()
	return conn, nil
}
