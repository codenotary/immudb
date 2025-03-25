/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
package document

import (
	"context"
	"errors"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"google.golang.org/protobuf/proto"

	"google.golang.org/protobuf/types/known/structpb"
)

type DocumentReader interface {
	// Read reads a single message from a reader and returns it as a Struct message.
	Read(ctx context.Context) (*protomodel.DocumentAtRevision, error)
	// ReadN reads n number of messages from a reader and returns them as a slice of Struct messages.
	ReadN(ctx context.Context, count int) ([]*protomodel.DocumentAtRevision, error)
	Close() error
}

type documentReader struct {
	rowReader       sql.RowReader
	onCloseCallback func(reader DocumentReader)
}

func newDocumentReader(rowReader sql.RowReader, onCloseCallback func(reader DocumentReader)) DocumentReader {
	return &documentReader{
		rowReader:       rowReader,
		onCloseCallback: onCloseCallback,
	}
}

// ReadN reads n number of messages from a reader and returns them as a slice of Struct messages.
func (r *documentReader) ReadN(ctx context.Context, count int) ([]*protomodel.DocumentAtRevision, error) {
	if count < 1 {
		return nil, sql.ErrIllegalArguments
	}

	revisions := make([]*protomodel.DocumentAtRevision, 0)

	var err error

	for l := 0; l < count; l++ {
		var row *sql.Row
		row, err = r.rowReader.Read(ctx)
		if errors.Is(err, sql.ErrNoMoreRows) {
			err = ErrNoMoreDocuments
			break
		}
		if err != nil {
			return nil, mayTranslateError(err)
		}

		docBytes := row.ValuesByPosition[0].RawValue().([]byte)

		doc := &structpb.Struct{}
		err = proto.Unmarshal(docBytes, doc)
		if err != nil {
			return nil, err
		}

		revisions = append(revisions, &protomodel.DocumentAtRevision{
			TransactionId: 0, // TODO: not yet available
			Revision:      0, // TODO: not yet available
			Document:      doc,
		})
	}

	return revisions, err
}

func (r *documentReader) Close() error {
	if r.onCloseCallback != nil {
		defer r.onCloseCallback(r)
	}

	return r.rowReader.Close()
}

// Read reads a single message from a reader and returns it as a Struct message.
func (r *documentReader) Read(ctx context.Context) (*protomodel.DocumentAtRevision, error) {
	var row *sql.Row
	row, err := r.rowReader.Read(ctx)
	if errors.Is(err, sql.ErrNoMoreRows) {
		err = ErrNoMoreDocuments
	}
	if err != nil {
		return nil, mayTranslateError(err)
	}

	docBytes := row.ValuesByPosition[0].RawValue().([]byte)

	doc := &structpb.Struct{}
	err = proto.Unmarshal(docBytes, doc)
	if err != nil {
		return nil, err
	}

	revision := &protomodel.DocumentAtRevision{
		TransactionId: 0, // TODO: not yet available
		Revision:      0, // TODO: not yet available
		Document:      doc,
	}

	return revision, err
}
