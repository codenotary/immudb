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
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/appendable/remoteapp"
	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/embedded/remotestorage/s3"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/errors"
	"github.com/rs/xid"
)

// this set of errors is grouped around remote storage identifier concept
// and covers multiple possible scenarios or remote storage configurations
var (
	ErrRemoteStorageDoesNotMatch = errors.New("remote storage does not match local files for identifiers")
	ErrNoStorageForIdentifier    = errors.New("remote storage does not exist, unable to retrieve identifier")
	ErrNoRemoteIdentifier        = errors.New("remote storage does not have expected identifier")
)

func (s *ImmuServer) createRemoteStorageInstance() (remotestorage.Storage, error) {
	if s.Options.RemoteStorageOptions.S3Storage {
		if s.Options.RemoteStorageOptions.S3RoleEnabled &&
			(s.Options.RemoteStorageOptions.S3AccessKeyID != "" || s.Options.RemoteStorageOptions.S3SecretKey != "") {
			return nil, s3.ErrKeyCredentialsProvided
		}

		// S3 storage
		return s3.Open(
			s.Options.RemoteStorageOptions.S3Endpoint,
			s.Options.RemoteStorageOptions.S3RoleEnabled,
			s.Options.RemoteStorageOptions.S3Role,
			s.Options.RemoteStorageOptions.S3AccessKeyID,
			s.Options.RemoteStorageOptions.S3SecretKey,
			s.Options.RemoteStorageOptions.S3BucketName,
			s.Options.RemoteStorageOptions.S3Location,
			s.Options.RemoteStorageOptions.S3PathPrefix,
			s.Options.RemoteStorageOptions.S3InstanceMetadataURL,
		)
	}

	return nil, nil
}

func (s *ImmuServer) initializeRemoteStorage(storage remotestorage.Storage) error {
	if storage == nil {
		if s.Options.RemoteStorageOptions.S3ExternalIdentifier {
			return ErrNoStorageForIdentifier
		}
		// No remote storage
		return nil
	}

	ctx := context.Background()

	hasRemoteIdentifier, err := storage.Exists(ctx, IDENTIFIER_FNAME)
	if err != nil {
		return err
	}

	if !hasRemoteIdentifier && s.Options.RemoteStorageOptions.S3ExternalIdentifier {
		return ErrNoRemoteIdentifier
	}

	localIdentifierFile := filepath.Join(s.Options.Dir, IDENTIFIER_FNAME)

	if hasRemoteIdentifier {
		remoteIDStream, err := storage.Get(ctx, IDENTIFIER_FNAME, 0, -1)
		if err != nil {
			return err
		}
		remoteID, err := ioutil.ReadAll(remoteIDStream)
		remoteIDStream.Close()
		if err != nil {
			return err
		}

		if !fileExists(localIdentifierFile) {
			err := ioutil.WriteFile(localIdentifierFile, remoteID, os.ModePerm)
			if err != nil {
				return err
			}
			s.UUID, err = xid.FromBytes(remoteID)
			if err != nil {
				return err
			}
		} else {
			localID, err := ioutil.ReadFile(localIdentifierFile)
			if err != nil {
				return err
			}

			if !bytes.Equal(remoteID, localID) {
				return ErrRemoteStorageDoesNotMatch
			}
		}
	}

	// Ensure all sub-folders are created, init code relies on this
	_, subFolders, err := storage.ListEntries(context.Background(), "")
	if err != nil {
		return err
	}
	for _, subFolder := range subFolders {
		err := os.MkdirAll(
			filepath.Join(s.Options.Dir, subFolder),
			store.DefaultFileMode,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *ImmuServer) updateRemoteUUID(remoteStorage remotestorage.Storage) error {
	ctx := context.Background()
	return remoteStorage.Put(ctx, IDENTIFIER_FNAME, filepath.Join(s.Options.Dir, IDENTIFIER_FNAME))
}

func (s *ImmuServer) storeOptionsForDB(name string, remoteStorage remotestorage.Storage, stOpts *store.Options) *store.Options {
	if remoteStorage != nil {
		stOpts.WithAppFactory(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			remoteAppOpts := remoteapp.DefaultOptions()
			remoteAppOpts.Options = *opts

			s3Path, err := getS3RemotePath(s.Options.Dir, rootPath, subPath)
			if err != nil {
				return nil, err
			}

			return remoteapp.Open(
				filepath.Join(rootPath, subPath),
				s3Path,
				remoteStorage,
				remoteAppOpts,
			)
		}).
			WithFileSize(1 << 20). // Reduce file size for better cache granularity
			WithAppRemoveFunc(func(rootPath, subPath string) error {
				s3Path, err := getS3RemotePath(s.Options.Dir, rootPath, subPath)
				if err != nil {
					return err
				}

				err = os.RemoveAll(filepath.Join(rootPath, subPath))
				if err != nil {
					return err
				}
				return remoteStorage.RemoveAll(context.Background(), s3Path)
			})

		Metrics.RemoteStorageKind.WithLabelValues(name, remoteStorage.Kind()).Set(1)
	} else {

		// No remote storage
		Metrics.RemoteStorageKind.WithLabelValues(name, "none").Set(1)
	}

	return stOpts
}

func getS3RemotePath(dir, rootPath, subPath string) (string, error) {
	baseDir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	baseDir += string(filepath.Separator)

	fsPath, err := filepath.Abs(filepath.Join(rootPath, subPath))
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(fsPath, baseDir) {
		return "", errors.New("path assertion failed")
	}

	return strings.ReplaceAll(
		fsPath[len(baseDir):]+"/",
		string(filepath.Separator), "/",
	), nil
}
