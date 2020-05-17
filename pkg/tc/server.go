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

package tc

import (
	"context"
	"encoding/json"
	"github.com/codenotary/immudb/pkg/logger"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	immuclient "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
)

func NewServer(options Options) (immutcServer *ImmuTcServer, err error) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	immutcServer = DefaultServer()

	immutcServer.Options = options

	cliOpts := &immuclient.Options{
		Address:            options.ImmudbAddress,
		Port:               options.ImmudbPort,
		HealthCheckRetries: 1,
		MTLs:               options.MTLs,
		MTLsOptions:        options.MTLsOptions,
		Auth:               false,
		Config:             "",
	}

	if options.Logfile != "" {
		var flogger logger.Logger
		if flogger, immutcServer.LogFile, err = logger.NewFileLogger("immutc ", options.Logfile); err == nil {
			immutcServer.WithLogger(flogger)
		} else {
			return nil, err
		}
	}
	var ic immuclient.ImmuClient
	if ic, err = immuclient.NewImmuClient(cliOpts); err != nil {
		immutcServer.Logger.Errorf("Unable to instantiate client %s", err)
		return nil, err
	}

	immutcServer.WithChecker(NewImmuTc(ic, immutcServer.Logger))

	return immutcServer, nil
}

// Start launch trust checker and status http server
func (s *ImmuTcServer) Start() (err error) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer s.LogFile.Close()
	s.Logger.Infof("starting immutc: %v", s.Options)
	if s.Options.Pidfile != "" {
		if s.Pid, err = server.NewPid(s.Options.Pidfile); err != nil {
			s.Logger.Errorf("failed to write pidfile: %s", err)
			return err
		}
	}

	go func() error {
		if err = s.Checker.Start(ctx); err != nil {
			return err
		}
		return nil
	}()

	s.HttpServer = s.newWebserver()
	go s.gracefullShutdown()

	s.Logger.Infof("Status server is ready to handle requests at %s/status", s.Options.Address+":"+strconv.Itoa(s.Options.Port))
	if err = s.HttpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		s.Logger.Errorf("Could not listen on %s: %v", s.Options.Address+":"+strconv.Itoa(s.Options.Port), err)
	}

	<-s.Done
	s.Logger.Infof("Trust checker stopped")

	return err
}

// Stop shutdown trust checker and status http server
func (s *ImmuTcServer) Stop() error {
	s.gracefullShutdown()
	return nil
}

func (s *ImmuTcServer) newWebserver() *http.Server {
	router := http.NewServeMux()
	router.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json")
		status := make(map[string]bool)
		status["status"] = s.Checker.GetStatus(r.Context())
		if err := json.NewEncoder(w).Encode(status); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		return
	})

	return &http.Server{
		Addr:         s.Options.Address + ":" + strconv.Itoa(s.Options.Port),
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}
}

func (s *ImmuTcServer) gracefullShutdown() {
	signal.Notify(s.Quit, os.Interrupt)
	<-s.Quit
	s.Logger.Infof("Trust checker is shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// shutdown trust che checker
	s.Checker.Stop(ctx)
	s.HttpServer.SetKeepAlivesEnabled(false)
	// shutdown http server
	if err := s.HttpServer.Shutdown(ctx); err != nil {
		s.Logger.Errorf("Could not gracefully shutdown the server: %v", err)
	}
	close(s.Done)
}
