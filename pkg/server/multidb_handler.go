/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
)

type multidbHandler struct {
	s *ImmuServer
}

func (s *ImmuServer) multidbHandler() sql.MultiDBHandler {
	return &multidbHandler{s}
}

func (h *multidbHandler) UseDatabase(ctx context.Context, db string) error {
	return ErrNotSupported
}

func (h *multidbHandler) CreateDatabase(ctx context.Context, db string) error {
	_, err := h.s.CreateDatabase(ctx, &schema.Database{DatabaseName: db})
	return err
}

func (h *multidbHandler) ListDatabases(ctx context.Context) ([]string, error) {
	res, err := h.s.DatabaseList(ctx, nil)
	if err != nil {
		return nil, err
	}

	dbs := make([]string, len(res.Databases))

	for i, db := range res.Databases {
		dbs[i] = db.DatabaseName
	}

	return dbs, nil
}
