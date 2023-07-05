package remoteapp

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/embedded/remotestorage/memory"
	"github.com/stretchr/testify/require"
)

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

func TestRemoteStorageReaderUnsupportedMethods(t *testing.T) {
	r := remoteStorageReader{}

	require.Panics(t, func() { r.Metadata() })
	require.Panics(t, func() { r.Size() })
	require.Panics(t, func() { r.Offset() })
	require.Panics(t, func() { r.SetOffset(0) })
	require.Panics(t, func() { r.Append([]byte{0}) })
	require.Panics(t, func() { r.DiscardUpto(0) })
	require.Panics(t, func() { r.CompressionFormat() })
	require.Panics(t, func() { r.CompressionLevel() })
	require.Panics(t, func() { r.Copy("/tmp") })
}

func TestRemoteStorageFlush(t *testing.T) {
	r := remoteStorageReader{}
	require.NoError(t, r.Flush())
}

func TestRemoteStorageSync(t *testing.T) {
	r := remoteStorageReader{}
	require.NoError(t, r.Sync())
}

func TestRemoteStorageSwitchToReadOnlyMode(t *testing.T) {
	r := remoteStorageReader{}
	require.NoError(t, r.SwitchToReadOnlyMode())
}

func TestRemoteStorageReadAt(t *testing.T) {
	m := memory.Open()
	storeData(t, m, "fl", []byte{
		0, 0, 0, 4, // Dummy empty header
		0, 0, 0, 0,
		1, 2, 3, 4, // Data, 4 bytes
	})

	r, err := openRemoteStorageReader(m, "fl")
	require.NoError(t, err)

	b := make([]byte, 4)
	n, err := r.ReadAt(b, 0)
	require.NoError(t, err)
	require.EqualValues(t, 4, n)
	require.Equal(t, []byte{1, 2, 3, 4}, b)

	n, err = r.ReadAt(make([]byte, 10), 0)
	require.EqualValues(t, 4, n)
	require.Equal(t, io.EOF, err)

	n, err = r.ReadAt(make([]byte, 2), 3)
	require.EqualValues(t, 1, n)
	require.Equal(t, io.EOF, err)

	n, err = r.ReadAt(make([]byte, 2), -1)
	require.EqualValues(t, 0, n)
	require.ErrorIs(t, err, ErrIllegalArguments)

	n, err = r.ReadAt(make([]byte, 2), 4)
	require.EqualValues(t, 0, n)
	require.Equal(t, io.EOF, err)

	n, err = r.ReadAt(make([]byte, 2), 5)
	require.EqualValues(t, 0, n)
	require.Equal(t, io.EOF, err)
}

func TestRemoteStorageCorruptedHeader(t *testing.T) {
	for _, d := range []struct {
		name  string
		bytes []byte
	}{
		{"less than 4 bytes", []byte{0, 0, 0}},
		{"not enough header bytes", []byte{0, 0, 0, 5, 0, 0, 0, 0}},
	} {
		t.Run(d.name, func(t *testing.T) {
			m := memory.Open()
			storeData(t, m, "fl", d.bytes)

			r, err := openRemoteStorageReader(m, "fl")
			require.ErrorIs(t, err, ErrCorruptedMetadata)
			require.Nil(t, r)
		})
	}
}

type remoteStorageErrorInjector struct {
	remotestorage.Storage
	err           error
	errDuringRead bool
}

func (r *remoteStorageErrorInjector) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	if r.errDuringRead {
		return ioutil.NopCloser(errReader{r.err}), nil
	}
	return nil, r.err
}

func TestRemoteStorageOpenError(t *testing.T) {
	for _, duringRead := range []bool{false, true} {
		m := &remoteStorageErrorInjector{
			Storage:       memory.Open(),
			err:           errors.New("Injected error"),
			errDuringRead: duringRead,
		}
		storeData(t, m, "fl", []byte{
			0, 0, 0, 4, // Dummy empty header
			0, 0, 0, 0,
			1, 2, 3, 4, // Data, 4 bytes
		})

		r, err := openRemoteStorageReader(m, "fl")
		require.Equal(t, m.err, err)
		require.Nil(t, r)
	}
}
