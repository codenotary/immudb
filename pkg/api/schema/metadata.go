package schema

import (
	"context"
	"errors"
)

const maxMetadataLen = 256

var (
	ErrEmptyMetadataKey   = errors.New("metadata key cannot be empty")
	ErrEmptyMetadataValue = errors.New("metadata value cannot be empty")
	ErrMetadataTooLarge   = errors.New("metadata exceeds maximum size")
	ErrCorruptedMetadata  = errors.New("corrupted metadata")
)

const (
	UserRequestMetadataKey = "usr"
	IpRequestMetadataKey   = "ip"
)

type Metadata map[string]string

func (m Metadata) Marshal() ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}

	var data [maxMetadataLen]byte

	off := 0
	for k, v := range m {
		data[off] = byte(len(k) - 1)
		data[off+1] = byte(len(v) - 1)

		off += 2
		copy(data[off:], []byte(k))
		off += len(k)

		copy(data[off:], []byte(v))
		off += len(v)
	}
	return data[:off], nil
}

func (m Metadata) validate() error {
	size := 0
	for k, v := range m {
		if len(k) == 0 {
			return ErrEmptyMetadataKey
		}

		if len(v) == 0 {
			return ErrEmptyMetadataValue
		}

		size += len(k) + len(v) + 2

		if size > maxMetadataLen {
			return ErrMetadataTooLarge
		}
	}
	return nil
}

func (m Metadata) Unmarshal(data []byte) error {
	off := 0
	for off <= len(data)-2 {
		keySize := int(data[off]) + 1
		valueSize := int(data[off+1]) + 1

		off += 2

		if off+keySize+valueSize > len(data) {
			return ErrCorruptedMetadata
		}

		m[string(data[off:off+keySize])] = string(data[off+keySize : off+keySize+valueSize])

		off += keySize + valueSize
	}

	if off != len(data) {
		return ErrCorruptedMetadata
	}
	return nil
}

type metadataKey struct{}

func ContextWithMetadata(ctx context.Context, md Metadata) context.Context {
	return context.WithValue(ctx, metadataKey{}, md)
}

func MetadataFromContext(ctx context.Context) Metadata {
	md, ok := ctx.Value(metadataKey{}).(Metadata)
	if !ok {
		return nil
	}
	return md
}
