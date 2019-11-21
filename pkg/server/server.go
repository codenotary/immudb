/*
Copyright 2019 vChain, Inc.

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

package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/golang/protobuf/ptypes/empty"

	"github.com/codenotary/immudb/pkg/db"
	"github.com/codenotary/immudb/pkg/schema"
)

func (s *ImmuServer) Run() error {
	listener, err := net.Listen(s.Options.Network, s.Options.Bind())
	if err != nil {
		return err
	}
	t, err := db.Open(
		db.DefaultOptions(
			filepath.Join(s.Options.Dir, s.Options.DbName),
		),
	)
	if err != nil {
		return err
	}
	server := DefaultServer().WithTopic(t)
	server.Logger.Infof("starting immudb %v", s.Options)
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		if err := server.Stop(); err != nil {
			fmt.Println(err)
		}
	}()
	schema.RegisterImmuServiceServer(server.GrpcServer, server)
	return server.GrpcServer.Serve(listener)
}

func (s *ImmuServer) Stop() error {
	s.Logger.Infof("stopping immudb %v", s.Options)
	s.GrpcServer.Stop()
	if s.Topic != nil {
		return s.Topic.Close()
	}
	return nil
}

func (s *ImmuServer) Set(ctx context.Context, sr *schema.SetRequest) (*empty.Empty, error) {
	s.Logger.Debugf("set %s %d bytes", sr.Key, len(sr.Value))
	if err := s.Topic.Set(sr.Key, sr.Value); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (s *ImmuServer) SetBatch(ctx context.Context, bsr *schema.BatchSetRequest) (*empty.Empty, error) {
	s.Logger.Debugf("set batch %d", len(bsr.SetRequests))
	var kvPairs []db.KVPair
	for _, sr := range bsr.SetRequests {
		kvPairs = append(kvPairs, db.KVPair{
			Key:   sr.Key,
			Value: sr.Value,
		})
	}
	if err := s.Topic.SetBatch(kvPairs); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (s *ImmuServer) Get(ctx context.Context, gr *schema.GetRequest) (*schema.GetResponse, error) {
	value, err := s.Topic.Get(gr.Key)
	s.Logger.Debugf("get %s %d bytes", gr.Key, len(value))
	if err != nil {
		return nil, err
	}
	return &schema.GetResponse{Value: value}, nil
}
