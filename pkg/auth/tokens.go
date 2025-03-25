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

package auth

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	immuerror "github.com/codenotary/immudb/pkg/errors"

	"github.com/o1egl/paseto"
	"github.com/rs/xid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var pasetoV2 = paseto.NewV2()

const footer = "immudb"

// GenerateToken ...
func GenerateToken(user User, database int64, expTime int) (string, error) {
	now := time.Now()
	keys, ok := getTokenForUser(user.Username)
	if !ok {
		if err := generateKeys(user.Username); err != nil {
			return "", err
		}
		keys, ok = getTokenForUser(user.Username)
		if !ok {
			return "", errors.New("internal error: missing auth keys")
		}
	} else {
		updateLastTokenGeneratedAt(user.Username)
	}
	jsonToken := paseto.JSONToken{
		Expiration: now.Add(time.Duration(expTime) * time.Minute),
		Subject:    user.Username,
	}
	jsonToken.Set("database", fmt.Sprintf("%d", database))
	token, err := pasetoV2.Sign(keys.privateKey, jsonToken, footer)
	if err != nil {
		return "", fmt.Errorf("error generating token: %v", err)
	}
	go evictOldTokenKeyPairs()
	return token, nil
}

// JSONToken ...
type JSONToken struct {
	Username      string
	Expiration    time.Time
	DatabaseIndex int64
}

var tokenEncoder = base64.RawURLEncoding

// parsePublicTokenPayload parses the public (unencrypted) token payload
// works even with expired tokens (that do not pass verification)
func parsePublicTokenPayload(token string) (*JSONToken, error) {
	tokenPieces := strings.Split(token, ".")
	if len(tokenPieces) < 3 {
		// version.purpose.payload or version.purpose.payload.footer
		// see: https://tools.ietf.org/id/draft-paragon-paseto-rfc-00.html#rfc.section.2
		return nil, errors.New("malformed token: expected at least 3 pieces")
	}
	encodedPayload := []byte(tokenPieces[2])
	payload := make([]byte, tokenEncoder.DecodedLen(len(encodedPayload)))
	if _, err := tokenEncoder.Decode(payload, encodedPayload); err != nil {
		return nil, fmt.Errorf("error decoding token payload: %v", err)
	}
	if len(payload) < ed25519.SignatureSize {
		return nil, errors.New("malformed token: incorrect token size")
	}
	payloadBytes := payload[:len(payload)-ed25519.SignatureSize]
	var jsonToken paseto.JSONToken
	if err := json.Unmarshal(payloadBytes, &jsonToken); err != nil {
		return nil, fmt.Errorf("error unmarshalling token payload json: %v", err)
	}
	var index int64 = -1
	if p := jsonToken.Get("database"); p != "" {
		pint, err := strconv.ParseInt(p, 10, 8)
		if err == nil {
			index = pint
		}
	}
	return &JSONToken{
		Username:      jsonToken.Subject,
		Expiration:    jsonToken.Expiration,
		DatabaseIndex: index,
	}, nil
}

func verifyToken(token string) (*JSONToken, error) {
	tokenPayload, err := parsePublicTokenPayload(token)
	if err != nil {
		return nil, err
	}
	keys, ok := getTokenForUser(tokenPayload.Username)
	if !ok {
		return nil, status.Error(
			codes.Unauthenticated, "Token data not found")
	}
	var jsonToken paseto.JSONToken
	var footer string
	if err := pasetoV2.Verify(token, keys.publicKey, &jsonToken, &footer); err != nil {
		return nil, err
	}
	if err := jsonToken.Validate(); err != nil {
		return nil, err
	}
	var index int64 = -1
	if p := jsonToken.Get("database"); p != "" {
		pint, err := strconv.ParseInt(p, 10, 64)
		if err == nil {
			index = pint
		}
	}
	return &JSONToken{
		Username:      jsonToken.Subject,
		Expiration:    jsonToken.Expiration,
		DatabaseIndex: index,
	}, nil
}

func verifyTokenFromCtx(ctx context.Context) (*JSONToken, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, ErrNotLoggedIn
	}
	authHeader, ok := md["authorization"]
	if !ok || len(authHeader) < 1 {
		return nil, ErrNotLoggedIn
	}
	token := strings.TrimPrefix(authHeader[0], "Bearer ")
	if token == "" {
		return nil, ErrNotLoggedIn
	}
	jsonToken, err := verifyToken(token)
	if err != nil {
		if strings.HasPrefix(fmt.Sprintf("%s", err), "token has expired") {
			return nil, err
		}
		if st, stOk := status.FromError(err); stOk {
			if st.Code() == codes.Unauthenticated {
				return nil, ErrNotLoggedIn
			}
		}
		return nil, immuerror.Wrap(err, "invalid token")
	}
	return jsonToken, nil
}

//NewUUID generate uuid
func NewUUID() xid.ID {
	return xid.New()
}

//NewStringUUID generate uuid and return as string
func NewStringUUID() string {
	return xid.New().String()
}
