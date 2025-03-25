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

package server

import (
	"context"
	"io/ioutil"
	"os"
	"path"

	"github.com/rs/xid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// IDENTIFIER_FNAME ...
const IDENTIFIER_FNAME = "immudb.identifier"

// SERVER_UUID_HEADER ...
const SERVER_UUID_HEADER = "immudb-uuid"

type uuidContext struct {
	UUID xid.ID
}

// UUIDContext manage UUID context
type UUIDContext interface {
	UUIDStreamContextSetter(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error
	UUIDContextSetter(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error)
}

// NewUUIDContext return a new UUId context servive
func NewUUIDContext(id xid.ID) uuidContext {
	return uuidContext{id}
}

// getOrSetUUID either reads the identifier file or creates it if it doesn't exist
// unless useExternalIdentifier is set to true, in which case the local identifier file is ignored.
// In earlier versions, the file was located inside default DB directory. Now, it
// is moved to the data directory. This function migrates the file to data directory
// in case it exists in the default db directory.
func getOrSetUUID(dataDir, defaultDbDir string, useExternalIdentifier bool) (xid.ID, error) {
	fileInDataDir := path.Join(dataDir, IDENTIFIER_FNAME)
	if fileExists(fileInDataDir) {
		return getUUID(fileInDataDir)
	}

	fileInDefaultDbDir := path.Join(defaultDbDir, IDENTIFIER_FNAME)
	if fileExists(fileInDefaultDbDir) {
		guid, err := getUUID(fileInDefaultDbDir)
		if err != nil {
			return guid, err
		}

		err = moveUUIDFile(guid, fileInDataDir, fileInDefaultDbDir)
		return guid, err
	}

	if useExternalIdentifier {
		return xid.ID{}, nil
	}

	guid := xid.New()
	err := setUUID(guid, fileInDataDir)
	return guid, err
}

func getUUID(fname string) (xid.ID, error) {
	b, err := ioutil.ReadFile(fname)
	if err != nil {
		return xid.ID{}, err
	}
	return xid.FromBytes(b)
}

func setUUID(guid xid.ID, fname string) error {
	return ioutil.WriteFile(fname, guid.Bytes(), os.ModePerm)
}

func moveUUIDFile(guid xid.ID, fileInDataDir, fileInDefaultDbDir string) error {
	// write the new file first in case we crash in between. Do not use
	// os.Rename here because in case of a crash, bad things can happen.
	if err := setUUID(guid, fileInDataDir); err != nil {
		return err
	}

	// now, delete the old file once we have new file setup
	err := os.Remove(fileInDefaultDbDir)
	return err
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// UUIDStreamContextSetter set uuid header in a stream
func (u *uuidContext) UUIDStreamContextSetter(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	header := metadata.Pairs(SERVER_UUID_HEADER, u.UUID.String())

	err := ss.SendHeader(header)
	if err != nil {
		return err
	}

	return handler(srv, ss)
}

// UUIDContextSetter set uuid header
func (u *uuidContext) UUIDContextSetter(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	header := metadata.Pairs(SERVER_UUID_HEADER, u.UUID.String())

	err := grpc.SendHeader(ctx, header)
	if err != nil {
		return nil, err
	}

	return handler(ctx, req)
}
