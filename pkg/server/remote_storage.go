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
)

var (
	ErrRemoteStorageDoesNotMatch = errors.New("remote storage does not match local files")
)

func (s *ImmuServer) createRemoteStorageInstance() (remotestorage.Storage, error) {
	if s.Options.RemoteStorageOptions.S3Storage {

		s.Logger.Warningf("Remote Storage is a work-in-progress feature. Not ready for production use")

		// S3 storage
		return s3.Open(
			s.Options.RemoteStorageOptions.S3Endpoint,
			s.Options.RemoteStorageOptions.S3AccessKeyID,
			s.Options.RemoteStorageOptions.S3SecretKey,
			s.Options.RemoteStorageOptions.S3BucketName,
			s.Options.RemoteStorageOptions.S3PathPrefix,
		)
	}

	return nil, nil

}

func (s *ImmuServer) initializeRemoteStorage(storage remotestorage.Storage) error {

	if storage == nil {
		// No remote storage
		return nil
	}

	ctx := context.Background()

	hasRemoteIdentifier, err := storage.Exists(ctx, IDENTIFIER_FNAME)
	if err != nil {
		return err
	}

	localIdentifierFile := filepath.Join(s.Options.Dir, IDENTIFIER_FNAME)

	if hasRemoteIdentifier {

		remoteIdStream, err := storage.Get(ctx, IDENTIFIER_FNAME, 0, -1)
		if err != nil {
			return err
		}
		remoteId, err := ioutil.ReadAll(remoteIdStream)
		remoteIdStream.Close()
		if err != nil {
			return err
		}

		if !fileExists(localIdentifierFile) {
			err := ioutil.WriteFile(localIdentifierFile, remoteId, os.ModePerm)
			if err != nil {
				return err
			}
		} else {
			localId, err := ioutil.ReadFile(localIdentifierFile)
			if err != nil {
				return err
			}

			if !bytes.Equal(remoteId, localId) {
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
			s.Options.StoreOptions.FileMode,
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

func (s *ImmuServer) storeOptionsForDb(name string, remoteStorage remotestorage.Storage) *store.Options {
	opts := *s.Options.StoreOptions

	if remoteStorage != nil {

		opts.WithAppFactory(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			baseDir, err := filepath.Abs(s.Options.Dir)
			if err != nil {
				return nil, err
			}
			baseDir += string(filepath.Separator)

			remoteAppOpts := remoteapp.DefaultOptions()
			remoteAppOpts.Options = *opts

			fsPath, err := filepath.Abs(filepath.Join(rootPath, subPath))
			if err != nil {
				return nil, err
			}
			if !strings.HasPrefix(fsPath, baseDir) {
				return nil, errors.New("path assertion failed")
			}
			s3Path := strings.ReplaceAll(
				fsPath[len(baseDir):]+"/",
				string(filepath.Separator), "/",
			)

			return remoteapp.Open(
				filepath.Join(rootPath, subPath),
				s3Path,
				remoteStorage,
				remoteAppOpts,
			)
		}).
			WithFileSize(1 << 20).       // Reduce file size for better cache granularity
			WithCompactionDisabled(true) // Disable index compaction
	}

	return &opts
}
