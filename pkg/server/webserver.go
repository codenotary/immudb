package server

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"net/http"
)

func StartWebServer(addr string, grpcAddr string, l logger.Logger) (*http.Server, error) {
	proxyMux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}
	l.Infof("Web API/Console server listening on %s. Managing immudb via %s", addr, grpcAddr)
	err := schema.RegisterImmuServiceHandlerFromEndpoint(context.Background(), proxyMux, grpcAddr, opts)
	if err != nil {
		return nil, err
	}

	webMux := http.NewServeMux()
	webMux.Handle("/api/", http.StripPrefix("/api", proxyMux))
	
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
