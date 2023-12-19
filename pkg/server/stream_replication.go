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
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"google.golang.org/grpc/metadata"
)

func (s *ImmuServer) ExportTx(req *schema.ExportTxRequest, txsServer schema.ImmuService_ExportTxServer) error {
	return s.exportTx(req, txsServer, true, make([]byte, s.Options.StreamChunkSize))
}

// StreamExportTx implements the bidirectional streaming endpoint used to export transactions
func (s *ImmuServer) StreamExportTx(stream schema.ImmuService_StreamExportTxServer) error {
	buf := make([]byte, s.Options.StreamChunkSize)

	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		err = s.exportTx(req, stream, false, buf)
		if err != nil {
			return err
		}
	}
}

func (s *ImmuServer) exportTx(req *schema.ExportTxRequest, txsServer schema.ImmuService_ExportTxServer, setTrailer bool, buf []byte) error {
	if req == nil || req.Tx == 0 || txsServer == nil {
		return ErrIllegalArguments
	}

	db, err := s.getDBFromCtx(txsServer.Context(), "ExportTx")
	if err != nil {
		return err
	}

	txbs, mayCommitUpToTxID, mayCommitUpToAlh, err := db.ExportTxByID(txsServer.Context(), req)
	if err != nil {
		return err
	}

	var streamMetadata map[string][]byte

	if req.ReplicaState != nil {
		var bMayCommitUpToTxID [8]byte
		binary.BigEndian.PutUint64(bMayCommitUpToTxID[:], mayCommitUpToTxID)

		var bCommittedTxID [8]byte
		state, err := db.CurrentState()
		if err == nil {
			binary.BigEndian.PutUint64(bCommittedTxID[:], state.TxId)
		}

		streamMetadata = map[string][]byte{
			"may-commit-up-to-txid-bin": bMayCommitUpToTxID[:],
			"may-commit-up-to-alh-bin":  mayCommitUpToAlh[:],
			"committed-txid-bin":        bCommittedTxID[:],
		}

		if setTrailer {
			// trailer metadata is kept for backward compatibility
			// it should not be sent when replication is done with bidirectional streaming
			// otherwise metadata will get accumulated over time
			md := metadata.Pairs(
				"may-commit-up-to-txid-bin", string(bMayCommitUpToTxID[:]),
				"may-commit-up-to-alh-bin", string(mayCommitUpToAlh[:]),
				"committed-txid-bin", string(bCommittedTxID[:]),
			)
			txsServer.SetTrailer(md)
		}
	}

	sender := stream.NewMsgSender(txsServer, buf)

	return sender.Send(bytes.NewReader(txbs), len(txbs), streamMetadata)
}

func (s *ImmuServer) ReplicateTx(replicateTxServer schema.ImmuService_ReplicateTxServer) error {
	if replicateTxServer == nil {
		return ErrIllegalArguments
	}

	ctx := replicateTxServer.Context()

	db, err := s.getDBFromCtx(ctx, "ReplicateTx")
	if err != nil {
		return err
	}

	if s.replicationInProgressFor(db.GetName()) {
		return ErrReplicationInProgress
	}

	var skipIntegrityCheck bool
	var waitForIndexing bool

	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if len(md.Get("skip-integrity-check")) > 0 {
			skipIntegrityCheck, err = strconv.ParseBool(md.Get("skip-integrity-check")[0])
			if err != nil {
				return err
			}
		}

		if len(md.Get("wait-for-indexing")) > 0 {
			waitForIndexing, err = strconv.ParseBool(md.Get("wait-for-indexing")[0])
			if err != nil {
				return err
			}
		}
	}

	receiver := s.StreamServiceFactory.NewMsgReceiver(replicateTxServer)

	bs, _, err := receiver.ReadFully()
	if err != nil {
		return err
	}

	hdr, err := db.ReplicateTx(replicateTxServer.Context(), bs, skipIntegrityCheck, waitForIndexing)
	if err != nil {
		return err
	}

	return replicateTxServer.SendAndClose(hdr)
}
