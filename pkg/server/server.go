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
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/db"
	"github.com/codenotary/immudb/pkg/schema"
)

func (s *ImmuServer) Start() (err error) {
	listener, err := net.Listen(s.Options.Network, s.Options.Bind())
	if err != nil {
		return err
	}
	dbDir := filepath.Join(s.Options.Dir, s.Options.DbName)
	if err := os.MkdirAll(dbDir, os.ModePerm); err != nil {
		return err
	}
	s.Topic, err = db.Open(db.DefaultOptions(dbDir))
	if err != nil {
		return err
	}
	s.GrpcServer = grpc.NewServer()
	schema.RegisterImmuServiceServer(s.GrpcServer, s)
	s.installShutdownHandler()
	s.Logger.Infof("starting immud: %v", s.Options)
	err = s.GrpcServer.Serve(listener)
	<-s.quit
	return
}

func (s *ImmuServer) Stop() error {
	s.Logger.Infof("stopping immud: %v", s.Options)
	defer func() { s.quit <- struct{}{} }()
	s.GrpcServer.Stop()
	s.GrpcServer = nil
	if s.Topic != nil {
		defer func() { s.Topic = nil }()
		return s.Topic.Close()
	}
	return nil
}

func (s *ImmuServer) Set(ctx context.Context, sr *schema.SetRequest) (*schema.SetResponse, error) {
	s.Logger.Debugf("set %s %d bytes", sr.Key, len(sr.Value))
	index, err := s.Topic.Set(sr.Key, sr.Value)
	if err != nil {
		return nil, err
	}
	return &schema.SetResponse{Index: index}, nil
}

func (s *ImmuServer) SetBatch(ctx context.Context, bsr *schema.BatchSetRequest) (*schema.SetResponse, error) {
	s.Logger.Debugf("set batch %d", len(bsr.SetRequests))
	var kvPairs []db.KVPair
	for _, sr := range bsr.SetRequests {
		kvPairs = append(kvPairs, db.KVPair{
			Key:   sr.Key,
			Value: sr.Value,
		})
	}
	index, err := s.Topic.SetBatch(kvPairs)
	if err != nil {
		return nil, err
	}
	return &schema.SetResponse{Index: index}, nil
}

func (s *ImmuServer) Get(ctx context.Context, gr *schema.GetRequest) (*schema.GetResponse, error) {
	value, index, err := s.Topic.Get(gr.Key)
	s.Logger.Debugf("get %s %d bytes", gr.Key, len(value))
	if err != nil {
		return nil, err
	}
	return &schema.GetResponse{Index: index, Value: value}, nil
}

func (s *ImmuServer) GetBatch(ctx context.Context, bgr *schema.BatchGetRequest) (*schema.BatchGetResponse, error) {
	var grs []*schema.GetResponse
	for _, gr := range bgr.GetRequests {
		gr, err := s.Get(ctx, gr)
		if err != nil {
			return nil, err
		}
		grs = append(grs, gr)
	}
	return &schema.BatchGetResponse{GetResponses: grs}, nil
}

func (s *ImmuServer) Health(context.Context, *empty.Empty) (*schema.HealthResponse, error) {
	health := s.Topic.HealthCheck()
	s.Logger.Debugf("health check: %v", health)
	return &schema.HealthResponse{Status: health}, nil
}

func (s *ImmuServer) installShutdownHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		s.Logger.Infof("caught SIGTERM")
		if err := s.Stop(); err != nil {
			s.Logger.Errorf("shutdown error: %v", err)
		}
		s.Logger.Infof("shutdown completed")
	}()
}
