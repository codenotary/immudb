package paseto

import (
	"bytes"
	"crypto/sha512"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/pkg/errors"
	"golang.org/x/crypto/hkdf"
)

/*
Format the Additional Associated Data.

Prefix with the length (64-bit unsigned little-endian integer)
followed by each message. This provides a more explicit domain
separation between each piece of the message.
*/
func preAuthEncode(pieces ...[]byte) []byte {
	buf := new(bytes.Buffer)
	byteOrder := binary.LittleEndian
	binary.Write(buf, byteOrder, int64(len(pieces)))
	for _, p := range pieces {
		binary.Write(buf, byteOrder, int64(len(p)))
		buf.Write(p)
	}
	return buf.Bytes()
}

func splitKey(key []byte, salt []byte) ([]byte, []byte, error) {
	eReader := hkdf.New(sha512.New384, key, salt, []byte("paseto-encryption-key"))
	aReader := hkdf.New(sha512.New384, key, salt, []byte("paseto-auth-key-for-aead"))
	encKey := make([]byte, 32)
	authKey := make([]byte, 32)
	if _, err := io.ReadFull(eReader, encKey); err != nil {
		return nil, nil, err
	}
	if _, err := io.ReadFull(aReader, authKey); err != nil {
		return nil, nil, err
	}

	return encKey, authKey, nil
}

func splitToken(token []byte, header []byte) (payload []byte, footer []byte, err error) {
	var (
		encodedPayload []byte
		encodedFooter  []byte
	)

	if !bytes.HasPrefix(token, header) {
		return nil, nil, ErrIncorrectTokenHeader
	}

	parts := bytes.Split(token[len(header):], []byte("."))
	switch len(parts) {
	case 1:
		encodedPayload = parts[0]
	case 2:
		encodedPayload = parts[0]
		encodedFooter = parts[1]
	default:
		return nil, nil, ErrIncorrectTokenFormat
	}

	payload = make([]byte, tokenEncoder.DecodedLen(len(encodedPayload)))
	if _, err = tokenEncoder.Decode(payload, encodedPayload); err != nil {
		return nil, nil, errors.Wrap(err, "failed to decode payload")
	}

	if encodedFooter != nil {
		footer = make([]byte, tokenEncoder.DecodedLen(len(encodedFooter)))
		if _, err = tokenEncoder.Decode(footer, encodedFooter); err != nil {
			return nil, nil, errors.Wrap(err, "failed to decode footer")
		}
	}

	return payload, footer, nil
}

func infToByteArr(i interface{}) ([]byte, error) {
	switch v := i.(type) {
	case string:
		return []byte(v), nil
	case *string:
		if v != nil {
			return []byte(*v), nil
		}
	case []byte:
		return v, nil
	case *[]byte:
		if v != nil {
			return *v, nil
		}
	default:
		return json.Marshal(v)
	}

	return nil, nil
}

func fillValue(data []byte, i interface{}) error {
	switch f := i.(type) {
	case *string:
		*f = string(data)
	case *[]byte:
		*f = append(*f, data...)
	default:
		if err := json.Unmarshal(data, i); err != nil {
			return ErrDataUnmarshal
		}
	}
	return nil
}

func createToken(header []byte, body []byte, footer []byte) string {
	encodedPayload := make([]byte, tokenEncoder.EncodedLen(len(body)))
	tokenEncoder.Encode(encodedPayload, body)

	footerLen := 0
	var encodedFooter []byte
	if len(footer) > 0 {
		encodedFooter = make([]byte, tokenEncoder.EncodedLen(len(footer)))
		tokenEncoder.Encode(encodedFooter, footer)
		footerLen = len(encodedFooter) + 1
	}

	token := make([]byte, len(header)+len(encodedPayload)+footerLen)

	offset := 0
	offset += copy(token[offset:], header)
	offset += copy(token[offset:], encodedPayload)
	if encodedFooter != nil {
		offset += copy(token[offset:], []byte("."))
		copy(token[offset:], encodedFooter)

	}
	return string(token)
}
