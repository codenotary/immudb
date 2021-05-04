package server

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/webconsole"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"net/http"
)

func StartWebServer(addr string, s schema.ImmuServiceServer, l logger.Logger) (*http.Server, error) {
	proxyMux := runtime.NewServeMux()
	err := schema.RegisterImmuServiceHandlerServer(context.Background(), proxyMux, s)
	if err != nil {
		return nil, err
	}

	webMux := http.NewServeMux()
	webMux.Handle("/api/", http.StripPrefix("/api", proxyMux))
	l.Infof("Web API server enabled on %s/api.", addr)

	err = webconsole.SetupWebconsole(webMux, l, addr)
	if err != nil {
		return nil, err
	}

	server := &http.Server{Addr: addr, Handler: webMux}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				l.Debugf("Web API/console http server closed")
			} else {
				l.Errorf("Web API/console error: %s", err)
			}

		}
	}()

	return server, nil
}
