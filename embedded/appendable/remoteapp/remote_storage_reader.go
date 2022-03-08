package remoteapp

import (
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/prometheus/client_golang/prometheus"
)

type remoteStorageReader struct {
	r          remotestorage.Storage
	name       string
	baseOffset int64
	dataCache  []byte // Initially we read the whole object into data cache
}

func openRemoteStorageReader(r remotestorage.Storage, name string) (*remoteStorageReader, error) {
	defer prometheus.NewTimer(metricsOpenTime).ObserveDuration()

	ctx := context.Background()

	// Read header
	reader, err := r.Get(ctx, name, 0, -1)
	if err != nil {
		metricsUncachedReadErrors.Inc()
		return nil, err
	}
	data, err := ioutil.ReadAll(reader)
	reader.Close()
	if err != nil {
		metricsUncachedReadErrors.Inc()
		return nil, err
	}
	metricsUncachedReads.Inc()
	metricsUncachedReadBytes.Add(float64(len(data)))
	if len(data) < 4 {
		metricsCorruptedMetadata.Inc()
		return nil, ErrCorruptedMetadata
	}

	// TODO: Read the metadata and validate it

	baseOffset := int64(4 + binary.BigEndian.Uint32(data[:4]))
	if baseOffset > int64(len(data)) {
		metricsCorruptedMetadata.Inc()
		return nil, ErrCorruptedMetadata
	}

	return &remoteStorageReader{
		r:          r,
		name:       name,
		baseOffset: baseOffset,
		dataCache:  data[baseOffset:],
	}, nil
}

func (r *remoteStorageReader) Metadata() []byte {
	panic("unimplemented")
}

func (r *remoteStorageReader) Size() (int64, error) {
	panic("unimplemented")
}

func (r *remoteStorageReader) Offset() int64 {
	panic("unimplemented")
}

func (r *remoteStorageReader) SetOffset(off int64) error {
	panic("unimplemented")
}

func (r *remoteStorageReader) DiscardUpto(off int64) error {
	panic("unimplemented")
}

func (r *remoteStorageReader) Append(bs []byte) (off int64, n int, err error) {
	panic("unimplemented")
}

func (r *remoteStorageReader) CompressionFormat() int {
	panic("unimplemented")
}

func (r *remoteStorageReader) CompressionLevel() int {
	panic("unimplemented")
}

func (r *remoteStorageReader) Flush() error {
	return nil
}

func (r *remoteStorageReader) Sync() error {
	return nil
}

func (r *remoteStorageReader) ReadAt(bs []byte, off int64) (int, error) {
	if off < 0 {
		return 0, ErrIllegalArguments
	}

	if off > int64(len(r.dataCache)) {
		return 0, io.EOF
	}

	readBytes := copy(bs, r.dataCache[off:])
	metricsReads.Inc()
	metricsReadBytes.Add(float64(readBytes))
	if readBytes < len(bs) {
		return readBytes, io.EOF
	}
	return readBytes, nil

	// reader, err := r.r.Get(context.TODO(), r.name, off+r.baseOffset, int64(len(bs)))
	// if err != nil {
	// 	return 0, err
	// }
	// n, err := return io.ReadAtLeast(reader, bs, 1)
	// if err != nil {
	// 	return n, err
	// }
	// if n < len(bs) {
	//	return n, io.EOF
	// }
	// return n, nil
}

func (r *remoteStorageReader) Close() error {
	return nil
}

func (r *remoteStorageReader) Copy(dstPath string) error {
	panic("unimplemented")
}

var _ appendable.Appendable = (*remoteStorageReader)(nil)
