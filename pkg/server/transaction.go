/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"github.com/golang/protobuf/ptypes/empty"
)

func (s *ImmuServer) BeginTx(ctx context.Context, request *schema.BeginTxRequest) (*schema.BeginTxResponse, error) {
	sessionID, err := sessions.GetSessionIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sess := s.SessManager.GetSession(sessionID)
	if request.ReadWrite {
		if sess.GetReadWriteTxOngoing() {
			return nil, ErrReadWriteTxOngoing
		}
		s.SessManager.GetSession(sessionID).SetReadWriteTxOngoing(true)
	}

	tx := sess.NewTransaction(request.ReadWrite)

	rollback := func() error {
		println("ROLLBACK!")
		return nil
	}

	tx.AddOnDeleteCallback("rollback", rollback)

	return &schema.BeginTxResponse{TransactionID: tx.GetID()}, nil
}

func (s *ImmuServer) TxScanner(ctx context.Context, request *schema.TxScannerRequest) (*schema.TxScanneReponse, error) {
	panic("implement me")
}

func (s *ImmuServer) Commit(ctx context.Context, empty *empty.Empty) (*empty.Empty, error) {
	panic("implement me")
}

func (s *ImmuServer) Rollback(ctx context.Context, empty *empty.Empty) (*empty.Empty, error) {
	panic("implement me")
}

func (s *ImmuServer) TxSet(ctx context.Context, request *schema.TxSetRequest) (*empty.Empty, error) {
	panic("implement me")
}

func (s *ImmuServer) TxGet(ctx context.Context, request *schema.TxKeyRequest) (*schema.KeyValue, error) {
	panic("implement me")
}
