package server

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
)

func (s *session) writeVersionInfo() error {
	cols := []*schema.Column{{Name: "version", Type: "VARCHAR"}}
	if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
		return err
	}
	rows := []*schema.Row{{
		Columns: []string{"version"},
		Values:  []*schema.SQLValue{{Value: &schema.SQLValue_S{S: pgmeta.PgsqlProtocolVersionMessage}}},
	}}
	if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
		return err
	}

	return nil
}
