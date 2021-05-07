package server

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/webconsole"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"net/http"
)

func StartWebServer(addr string, s *ImmuServer, l logger.Logger) (*http.Server, error) {
	proxyMux := runtime.NewServeMux()
	err := schema.RegisterImmuServiceHandlerServer(context.Background(), proxyMux, s)
	if err != nil {
		return nil, err
	}

	webMux := http.NewServeMux()
	webMux.Handle("/api/", http.StripPrefix("/api", proxyMux))

	err = webconsole.SetupWebconsole(webMux, l, addr)
	if err != nil {
		return nil, err
	}

	server := &http.Server{Addr: addr, Handler: webMux}
	server.TLSConfig = s.Options.TLSConfig

	go func() {
		var err error
		if s.Options.TLSConfig != nil {
			l.Infof("Web API server enabled on %s/api (https)", addr)
			err = server.ListenAndServeTLS("", "")
		} else {
			l.Infof("Web API server enabled on %s/api (http)", addr)
			err = server.ListenAndServe()
		}

		if err == http.ErrServerClosed {
			l.Debugf("Web API/console server closed")
		} else {
			l.Errorf("Web API/console error: %s", err)
		}
	}()

	return server, nil
}
