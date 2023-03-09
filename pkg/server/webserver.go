package server

import (
	"context"
	"crypto/tls"
	"net/http"

	"github.com/codenotary/immudb/pkg/api/authorizationschema"
	"github.com/codenotary/immudb/pkg/api/documentschema"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/swagger"
	"github.com/codenotary/immudb/webconsole"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

func StartWebServer(addr string, tlsConfig *tls.Config, s *ImmuServer, l logger.Logger) (*http.Server, error) {
	proxyMux := runtime.NewServeMux()
	err := schema.RegisterImmuServiceHandlerServer(context.Background(), proxyMux, s)
	if err != nil {
		return nil, err
	}

	proxyMuxV2 := runtime.NewServeMux()
	err = documentschema.RegisterDocumentServiceHandlerServer(context.Background(), proxyMuxV2, s)
	if err != nil {
		return nil, err
	}

	err = authorizationschema.RegisterAuthorizationServiceHandlerServer(context.Background(), proxyMuxV2, s)
	if err != nil {
		return nil, err
	}

	webMux := http.NewServeMux()
	webMux.Handle("/api/", http.StripPrefix("/api", proxyMux))
	webMux.Handle("/api/v2/", http.StripPrefix("/api/v2", proxyMuxV2))

	err = webconsole.SetupWebconsole(webMux, l, addr)
	if err != nil {
		return nil, err
	}
	if s.Options.SwaggerUIEnabled {
		err = swagger.SetupSwaggerUI(webMux, l, addr)
		if err != nil {
			return nil, err
		}
	}

	httpServer := &http.Server{Addr: addr, Handler: webMux}
	httpServer.TLSConfig = tlsConfig

	go func() {
		var err error
		if tlsConfig != nil && len(tlsConfig.Certificates) > 0 {
			l.Infof("Web API server enabled on %s/api (https)", addr)
			err = httpServer.ListenAndServeTLS("", "")
		} else {
			l.Infof("Web API server enabled on %s/api (http)", addr)
			err = httpServer.ListenAndServe()
		}

		if err == http.ErrServerClosed {
			l.Debugf("Web API/console server closed")
		} else {
			l.Errorf("Web API/console error: %s", err)
		}
	}()

	return httpServer, nil
}
