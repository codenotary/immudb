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
package remotestorage

import (
	"context"
	"errors"
	"io"
)

var (
	ErrNotFound = errors.New("object not found")
)

type EntryInfo struct {
	Name string
	Size int64
}

type Storage interface {
	// Kind returns the kind of remote storage, e.g. `s3`
	Kind() string

	// String returns a human-readable representation of the storage
	String() string

	// Get opens a remote resource, if size < 0, read as much as possible
	Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error)

	// Put saves a local file to a remote storage
	Put(ctx context.Context, name string, fileName string) error

	// Exists checks if a remove resource exists and can be read.
	// Note that due to an asynchronous nature of cluod storage,
	// a resource stored with the Put method may not be immediately accessible.
	Exists(ctx context.Context, name string) (bool, error)

	// ListEntries list all entries available in the remote storage,
	// Entries must be sorted alphabetically
	ListEntries(ctx context.Context, path string) (entries []EntryInfo, subPaths []string, err error)
}
