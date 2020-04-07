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

package gw

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/rs/cors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func (s *ImmuGwServer) Start() error {
	opts := []grpc.DialOption{grpc.WithInsecure()}

	//---------- TLS Setting -----------//
	if s.Options.MTLs {
		//LoadX509KeyPair reads and parses a public/private key pair from a pair of files.
		//The files must contain PEM encoded data.
		//The certificate file may contain intermediate certificates following the leaf certificate to form a certificate chain.
		//On successful return, Certificate.Leaf will be nil because the parsed form of the certificate is not retained.
		cert, err := tls.LoadX509KeyPair(
			//certificate signed by intermediary for the client. It contains the public key.
			s.Options.MTLsOptions.Certificate,
			//client key (needed to sign the requests. Only the public key can open the data)
			s.Options.MTLsOptions.Pkey,
		)
		if err != nil {
			grpclog.Errorf("failed to read credentials: %s", err)
		}
		certPool := x509.NewCertPool()
		// chain is composed by default by ca.cert.pem and intermediate.cert.pem
		bs, err := ioutil.ReadFile(s.Options.MTLsOptions.ClientCAs)
		if err != nil {
			grpclog.Errorf("failed to read ca cert: %s", err)
		}

		// AppendCertsFromPEM attempts to parse a series of PEM encoded certificates.
		// It appends any certificates found to s and reports whether any certificates were successfully parsed.
		// On many Linux systems, /etc/ssl/cert.pem will contain the system wide set of root CAs
		// in a format suitable for this function.
		ok := certPool.AppendCertsFromPEM(bs)
		if !ok {
			grpclog.Errorf("failed to append certs")
		}

		transportCreds := credentials.NewTLS(&tls.Config{
			// ServerName is used to verify the hostname on the returned
			// certificates unless InsecureSkipVerify is given. It is also included
			// in the client's handshake to support virtual hosting unless it is
			// an IP address.
			ServerName: s.Options.MTLsOptions.Servername,
			// Certificates contains one or more certificate chains to present to the
			// other side of the connection. The first certificate compatible with the
			// peer's requirements is selected automatically.
			// Server configurations must set one of Certificates, GetCertificate or
			// GetConfigForClient. Clients doing client-authentication may set either
			// Certificates or GetClientCertificate.
			Certificates: []tls.Certificate{cert},
			// Safe store, trusted certificate list
			// Server need to use one certificate presents in this lists.
			// RootCAs defines the set of root certificate authorities
			// that clients use when verifying server certificates.
			// If RootCAs is nil, TLS uses the host's root CA set.
			RootCAs: certPool,
		})
		opts = []grpc.DialOption{grpc.WithTransportCredentials(transportCreds)}
	}

	grpcServerEndpoint := flag.String("grpc-server-endpoint", s.Options.ImmudAddress+":"+strconv.Itoa(s.Options.ImmudPort), "gRPC server endpoint")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := runtime.NewServeMux()

	handler := cors.Default().Handler(mux)

	conn, err := grpc.Dial(*grpcServerEndpoint, opts...)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", grpcServerEndpoint, cerr)
			}
			return
		}
		go func() {
			<-ctx.Done()
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", grpcServerEndpoint, cerr)
			}
		}()
	}()

	s.Client = schema.NewImmuServiceClient(conn)
	s.RootService = immuclient.NewRootService(s.Client, cache.NewFileCache())

	var root *schema.Root
	root, err = s.RootService.GetRoot(ctx)
	if err != nil {
		return err
	}
	ssh := NewSafesetHandler(mux, s.Client, s.RootService)
	sgh := NewSafegetHandler(mux, s.Client, s.RootService)
	hh := NewHistoryHandler(mux, s.Client, s.RootService)
	sr := NewSafeReferenceHandler(mux, s.Client, s.RootService)
	sza := NewSafeZAddHandler(mux, s.Client, s.RootService)
	mux.Handle(http.MethodPost, schema.Pattern_ImmuService_SafeSet_0(), ssh.Safeset)
	mux.Handle(http.MethodPost, schema.Pattern_ImmuService_SafeGet_0(), sgh.Safeget)
	mux.Handle(http.MethodGet, schema.Pattern_ImmuService_History_0(), hh.History)
	mux.Handle(http.MethodPost, schema.Pattern_ImmuService_SafeReference_0(), sr.SafeReference)
	mux.Handle(http.MethodPost, schema.Pattern_ImmuService_SafeZAdd_0(), sza.SafeZAdd)

	err = schema.RegisterImmuServiceHandlerFromEndpoint(ctx, mux, *grpcServerEndpoint, opts)
	if err != nil {
		return err
	}

	var protoReq empty.Empty
	var metadata runtime.ServerMetadata
	if health, err := s.Client.Health(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD)); err != nil {
		s.Logger.Infof(err.Error())
		return err
	} else {
		if !health.GetStatus() {
			msg := fmt.Sprintf("Immudb not in health at %s:%d", s.Options.ImmudAddress, s.Options.ImmudPort)
			s.Logger.Infof(msg)
			return errors.New(msg)
		} else {
			s.Logger.Infof(fmt.Sprintf("Immudb is listening at %s:%d", s.Options.ImmudAddress, s.Options.ImmudPort))
		}
	}

	s.installShutdownHandler()
	s.Logger.Infof("Starting immugw at %s:%d", s.Options.Address, s.Options.Port)

	if s.Options.Pidfile != "" {
		if s.Pid, err = server.NewPid(s.Options.Pidfile); err != nil {
			return err
		}
	}

	s.Logger.Infof("Root hash %x at %d", root.Root, root.Index)
	go func() {
		if err = http.ListenAndServe(s.Options.Address+":"+strconv.Itoa(s.Options.Port), handler); err != nil && err != http.ErrServerClosed {
			s.Logger.Errorf("Unable to launch immugw:%+s\n", err)
		}
	}()
	<-s.quit
	return err
}

func (s *ImmuGwServer) Stop() error {
	s.Logger.Infof("stopping immugw: %v", s.Options)
	return nil
}

func (s *ImmuGwServer) installShutdownHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		defer func() {
			s.quit <- struct{}{}
		}()
		<-c
		s.Logger.Infof("caught SIGTERM")
		if err := s.Stop(); err != nil {
			s.Logger.Errorf("shutdown error: %v", err)
		}
		s.Logger.Infof("shutdown completed")
	}()
}
