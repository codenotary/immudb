package server

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/embedded/remotestorage/memory"
	"github.com/codenotary/immudb/embedded/remotestorage/s3"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type remoteStorageMockingWrapper struct {
	wrapped remotestorage.Storage

	fnGet         func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error)
	fnPut         func(ctx context.Context, name string, fileName string, next func() error) error
	fnExists      func(ctx context.Context, name string, next func() (bool, error)) (bool, error)
	fnListEntries func(ctx context.Context, path string, next func() (entries []remotestorage.EntryInfo, subPaths []string, err error)) (entries []remotestorage.EntryInfo, subPaths []string, err error)
}

func (r *remoteStorageMockingWrapper) String() string {
	return r.wrapped.String()
}

func (r *remoteStorageMockingWrapper) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	if r.fnGet != nil {
		return r.fnGet(ctx, name, offs, size, func() (io.ReadCloser, error) {
			return r.wrapped.Get(ctx, name, offs, size)
		})
	}
	return r.wrapped.Get(ctx, name, offs, size)
}

func (r *remoteStorageMockingWrapper) Put(ctx context.Context, name string, fileName string) error {
	if r.fnPut != nil {
		return r.fnPut(ctx, name, fileName, func() error {
			return r.wrapped.Put(ctx, name, fileName)
		})
	}
	return r.wrapped.Put(ctx, name, fileName)
}

func (r *remoteStorageMockingWrapper) Exists(ctx context.Context, name string) (bool, error) {
	if r.fnExists != nil {
		return r.fnExists(ctx, name, func() (bool, error) {
			return r.wrapped.Exists(ctx, name)
		})
	}
	return r.wrapped.Exists(ctx, name)
}

func (r *remoteStorageMockingWrapper) ListEntries(ctx context.Context, path string) (entries []remotestorage.EntryInfo, subPaths []string, err error) {
	if r.fnListEntries != nil {
		return r.fnListEntries(ctx, path, func() (entries []remotestorage.EntryInfo, subPaths []string, err error) {
			return r.wrapped.ListEntries(ctx, path)
		})
	}
	return r.wrapped.ListEntries(ctx, path)
}

func TestCreateRemoteStorage(t *testing.T) {
	s := DefaultServer()

	// No remote storage by default
	storage, err := s.createRemoteStorageInstance()
	require.NoError(t, err)
	require.Nil(t, storage)

	// Set remote storage options
	s.WithOptions(DefaultOptions().WithRemoteStorageOptions(
		DefaultRemoteStorageOptions().
			WithS3Storage(true),
	))

	storage, err = s.createRemoteStorageInstance()
	require.NoError(t, err)
	require.NotNil(t, storage)
	require.IsType(t, &s3.Storage{}, storage)
}

func tmpFile(t *testing.T, data []byte) (fileName string, cleanup func()) {
	fl, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	_, err = fl.Write(data)
	require.NoError(t, err)
	err = fl.Close()
	require.NoError(t, err)
	return fl.Name(), func() {
		os.Remove(fl.Name())
	}
}

func storeData(t *testing.T, s remotestorage.Storage, name string, data []byte) {
	fl, c := tmpFile(t, data)
	defer c()

	err := s.Put(context.Background(), name, fl)
	require.NoError(t, err)
}

func TestInitializeRemoteStorageNoRemoteStorage(t *testing.T) {

	s := DefaultServer()
	err := s.initializeRemoteStorage(nil)
	require.NoError(t, err)
}

func TestInitializeRemoteStorageEmptyRemoteStorage(t *testing.T) {
	s := DefaultServer()
	err := s.initializeRemoteStorage(memory.Open())
	require.NoError(t, err)
}

func TestInitializeRemoteStorageEmptyRemoteStorageErrorOnExists(t *testing.T) {
	s := DefaultServer()
	injectedErr := errors.New("Injected error")
	mem := &remoteStorageMockingWrapper{
		wrapped: memory.Open(),
		fnExists: func(ctx context.Context, name string, next func() (bool, error)) (bool, error) {
			return false, injectedErr
		},
	}
	err := s.initializeRemoteStorage(mem)
	require.True(t, errors.Is(err, injectedErr))
}

func TestInitializeRemoteStorageEmptyRemoteStorageErrorOnListEntries(t *testing.T) {
	s := DefaultServer()
	injectedErr := errors.New("Injected error")
	mem := &remoteStorageMockingWrapper{
		wrapped: memory.Open(),
		fnListEntries: func(ctx context.Context, path string, next func() (entries []remotestorage.EntryInfo, subPaths []string, err error)) (entries []remotestorage.EntryInfo, subPaths []string, err error) {
			return nil, nil, injectedErr
		},
	}
	err := s.initializeRemoteStorage(mem)
	require.True(t, errors.Is(err, injectedErr))
}

func TestInitializeRemoteStorageDownloadIdentifier(t *testing.T) {
	require.NoError(t, os.MkdirAll("data", 0777))
	defer os.RemoveAll("data")

	s := DefaultServer()
	m := memory.Open()
	storeData(t, m, "immudb.identifier", []byte{1, 2, 3, 4, 5})

	err := s.initializeRemoteStorage(m)
	require.NoError(t, err)

	require.FileExists(t, "data/immudb.identifier")

	id, err := ioutil.ReadFile("data/immudb.identifier")
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3, 4, 5}, id)
}

func TestInitializeRemoteStorageDownloadIdentifierErrorOnGet(t *testing.T) {
	require.NoError(t, os.MkdirAll("data", 0777))
	defer os.RemoveAll("data")

	s := DefaultServer()
	injectedErr := errors.New("Injected error")
	m := &remoteStorageMockingWrapper{
		wrapped: memory.Open(),
		fnGet: func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error) {
			return nil, injectedErr
		},
	}
	storeData(t, m, "immudb.identifier", []byte{1, 2, 3, 4, 5})
	err := s.initializeRemoteStorage(m)
	require.True(t, errors.Is(err, injectedErr))
}

func TestInitializeRemoteStorageDownloadIdentifierErrorOnStore(t *testing.T) {
	require.NoError(t, os.MkdirAll("data/immudb.identifier", 0777))
	defer os.RemoveAll("data")

	s := DefaultServer()
	m := memory.Open()
	storeData(t, m, "immudb.identifier", []byte{1, 2, 3, 4, 5})
	err := s.initializeRemoteStorage(m)
	require.Error(t, err)
}

type errReader struct {
	err error
}

func (e errReader) Read([]byte) (int, error) {
	return 0, e.err
}

func TestInitializeRemoteStorageDownloadIdentifierErrorOnRead(t *testing.T) {
	require.NoError(t, os.MkdirAll("data", 0777))
	defer os.RemoveAll("data")

	s := DefaultServer()
	injectedErr := errors.New("Injected error")
	m := &remoteStorageMockingWrapper{
		wrapped: memory.Open(),
		fnGet: func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error) {
			return ioutil.NopCloser(errReader{injectedErr}), nil
		},
	}
	storeData(t, m, "immudb.identifier", []byte{1, 2, 3, 4, 5})
	err := s.initializeRemoteStorage(m)
	require.True(t, errors.Is(err, injectedErr))
}

func TestInitializeRemoteStorageIdentifierMismatch(t *testing.T) {
	require.NoError(t, os.MkdirAll("data", 0777))
	defer os.RemoveAll("data")

	s := DefaultServer()
	m := memory.Open()
	storeData(t, m, "immudb.identifier", []byte{1, 2, 3, 4, 5})

	_, err := getOrSetUUID("./data", "./data")
	require.NoError(t, err)

	err = s.initializeRemoteStorage(m)
	require.Equal(t, ErrRemoteStorageDoesNotMatch, err)
}

func TestInitializeRemoteStorageCreateLocalDirs(t *testing.T) {
	require.NoError(t, os.MkdirAll("data", 0777))
	defer os.RemoveAll("data")

	s := DefaultServer()
	m := memory.Open()
	storeData(t, m, "dir1/file1", []byte{1, 2, 3})
	storeData(t, m, "dir1/file2", []byte{1, 2, 3})
	storeData(t, m, "dir2/file3", []byte{1, 2, 3})
	storeData(t, m, "dir3/file4", []byte{1, 2, 3})

	err := s.initializeRemoteStorage(m)
	require.NoError(t, err)

	require.DirExists(t, "./data/dir1")
	require.DirExists(t, "./data/dir2")
	require.DirExists(t, "./data/dir3")
}

func TestInitializeRemoteStorageCreateLocalDirsError(t *testing.T) {
	require.NoError(t, os.MkdirAll("data", 0777))
	defer os.RemoveAll("data")

	s := DefaultServer()
	m := memory.Open()
	storeData(t, m, "dir1/file1", []byte{1, 2, 3})
	storeData(t, m, "dir1/file2", []byte{1, 2, 3})
	storeData(t, m, "dir2/file3", []byte{1, 2, 3})
	storeData(t, m, "dir3/file4", []byte{1, 2, 3})

	err := ioutil.WriteFile("./data/dir3", []byte{}, 0777)
	require.NoError(t, err)

	err = s.initializeRemoteStorage(m)
	require.Error(t, err)
}

func TestUpdateRemoteUUID(t *testing.T) {
	require.NoError(t, os.MkdirAll("data", 0777))
	defer os.RemoveAll("data")

	s := DefaultServer()
	m := memory.Open()

	uuid, err := getOrSetUUID("./data", "./data")
	require.NoError(t, err)
	s.UUID = uuid

	err = s.updateRemoteUUID(m)
	require.NoError(t, err)

	exists, err := m.Exists(context.Background(), "immudb.identifier")
	require.NoError(t, err)
	require.True(t, exists)

	data, err := m.Get(context.Background(), "immudb.identifier", 0, -1)
	require.NoError(t, err)
	defer data.Close()
	readUUID, err := ioutil.ReadAll(data)
	require.NoError(t, err)
	require.Equal(t, uuid.Bytes(), readUUID)
}

func TestStoreOptionsForDBWithRemoteStorage(t *testing.T) {
	require.NoError(t, os.MkdirAll("data/testdb", 0777))
	defer os.RemoveAll("data/testdb")

	s := DefaultServer()
	s.remoteStorage = memory.Open()

	stOpts := s.databaseOptionsFrom(s.defaultDBOptions("testdb")).GetStoreOptions()

	st, err := store.Open("data/testdb", stOpts)
	require.NoError(t, err)

	tx, err := st.NewWriteOnlyTx()
	require.NoError(t, err)

	err = tx.Set([]byte{1}, nil, []byte{2})
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)

	err = st.Close()
	require.NoError(t, err)

	// Ensure the data was written to the remote storage
	for _, name := range []string{
		"testdb/aht/commit/00000000.di",
		"testdb/aht/data/00000000.dat",
		"testdb/aht/tree/00000000.sha",
		"testdb/commit/00000000.txi",
		"testdb/index/commit/00000000.ri",
		"testdb/index/history/00000000.hx",
		"testdb/index/nodes/00000000.n",
		"testdb/tx/00000000.tx",
		"testdb/val_0/00000000.val",
	} {
		t.Run(name, func(t *testing.T) {
			exists, err := s.remoteStorage.Exists(context.Background(), name)
			require.NoError(t, err)
			require.True(t, exists)
		})
	}
}

func TestRemoteStorageUsedForNewDB(t *testing.T) {
	require.NoError(t, os.RemoveAll("data"))
	require.NoError(t, os.MkdirAll("data", 0777))
	defer os.RemoveAll("data")

	s := DefaultServer()
	s.WithOptions(DefaultOptions().
		WithPort(0).
		WithListener(bufconn.Listen(1024 * 1024)),
	)
	err := s.Initialize()
	require.NoError(t, err)

	m := memory.Open()
	s.remoteStorage = m

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	newdb := &schema.DatabaseSettings{
		DatabaseName: "newdb",
	}

	_, err = s.CreateDatabaseWith(ctx, newdb)
	require.NoError(t, err)
	err = s.CloseDatabases()
	require.NoError(t, err)

	exists, err := m.Exists(context.Background(), "newdb/tx/00000000.tx")
	require.NoError(t, err)
	require.True(t, exists)
}
