/*
Copyright 2019-2020 vChain, Inc.

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

package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/o1egl/paseto"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/ed25519"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Kind uint32

const (
	KindNone Kind = iota
	KindPassword
	KindCryptoSig
)

// TODO OGG: in the future, after other types of auth will be implemented,
// this will have to be of Kind (see above) type instead of bool:
var AuthEnabled bool

// GeneratePassword generates a random ASCII string with at least one digit and one special character
func GeneratePassword() string {
	rand.Seed(time.Now().UnixNano())
	digits := "0123456789"
	// other special characters: ~=+%^*/()[]{}/!@#$?|
	specials := "!?"
	all := "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		digits + specials
	length := 32
	buf := make([]byte, length)
	buf[0] = digits[rand.Intn(len(digits))]
	buf[1] = specials[rand.Intn(len(specials))]
	for i := 2; i < length; i++ {
		buf[i] = all[rand.Intn(len(all))]
	}
	rand.Shuffle(len(buf), func(i, j int) {
		buf[i], buf[j] = buf[j], buf[i]
	})
	return string(buf)
}

func HashAndSaltPassword(plainPassword string) ([]byte, error) {
	hashedPasswordBytes, err := bcrypt.GenerateFromPassword([]byte(plainPassword), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("error hashing password: %v", err)
	}
	return hashedPasswordBytes, nil
}

func ComparePasswords(hashedPassword []byte, plainPassword []byte) error {
	return bcrypt.CompareHashAndPassword(hashedPassword, plainPassword)
}

var pasetoV2 = paseto.NewV2()

type tokenKeyPair struct {
	publicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey
}

var tokenKeyPairs = struct {
	keysPerUser map[string]*tokenKeyPair
	sync.RWMutex
}{
	keysPerUser: map[string]*tokenKeyPair{},
}

// GenerateKeys ...
func GenerateKeys(username string) error {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return fmt.Errorf("error generating public and private key pair for user %s: %v", username, err)
	}
	tokenKeyPairs.Lock()
	defer tokenKeyPairs.Unlock()
	tokenKeyPairs.keysPerUser[username] = &tokenKeyPair{publicKey, privateKey}
	return nil
}

// DropTokenKeys ...
func DropTokenKeys(username string) bool {
	tokenKeyPairs.Lock()
	defer tokenKeyPairs.Unlock()
	_, ok := tokenKeyPairs.keysPerUser[username]
	if ok {
		delete(tokenKeyPairs.keysPerUser, username)
	}
	return ok
}

func DropTokenKeysForCtx(ctx context.Context) (bool, error) {
	jsonToken, err := verifyTokenFromCtx(ctx)
	if err != nil {
		return false, err
	}
	return DropTokenKeys(jsonToken.Username), nil
}

const footer = "immudb"
const tokenValidity = 1 * time.Hour

// GenerateToken ...
func GenerateToken(user User) (string, error) {
	keys, ok := tokenKeyPairs.keysPerUser[user.Username]
	if !ok {
		if err := GenerateKeys(user.Username); err != nil {
			return "", err
		}
		keys, ok = tokenKeyPairs.keysPerUser[user.Username]
		if !ok {
			return "", errors.New("internal error: missing auth keys")
		}
	}
	now := time.Now()
	jsonToken := paseto.JSONToken{
		Expiration: now.Add(tokenValidity),
		Subject:    user.Username,
	}
	jsonToken.Set("permissions", fmt.Sprintf("%d", user.Permissions))
	token, err := pasetoV2.Sign(keys.privateKey, jsonToken, footer)
	if err != nil {
		return "", fmt.Errorf("error generating token: %v", err)
	}
	return token, nil
}

// JSONToken ...
type JSONToken struct {
	Username    string
	Permissions byte
	Expiration  time.Time
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
	var permissions byte = PermissionR
	if p := jsonToken.Get("permissions"); p != "" {
		pint, err := strconv.ParseUint(p, 10, 8)
		if err == nil {
			permissions = byte(pint)
		}
	}
	return &JSONToken{
		Username:    jsonToken.Subject,
		Permissions: permissions,
		Expiration:  jsonToken.Expiration,
	}, nil
}

func verifyToken(token string) (*JSONToken, error) {
	tokenPayload, err := parsePublicTokenPayload(token)
	if err != nil {
		return nil, err
	}
	keys, ok := tokenKeyPairs.keysPerUser[tokenPayload.Username]
	if !ok {
		return nil, status.Error(
			codes.Unauthenticated, "invalid token")
	}
	var jsonToken paseto.JSONToken
	var footer string
	if err := pasetoV2.Verify(token, keys.publicKey, &jsonToken, &footer); err != nil {
		return nil, err
	}
	if err := jsonToken.Validate(); err != nil {
		return nil, err
	}
	var permissions byte = PermissionR
	if p := jsonToken.Get("permissions"); p != "" {
		pint, err := strconv.ParseUint(p, 10, 8)
		if err == nil {
			permissions = byte(pint)
		}
	}
	return &JSONToken{
		Username:    jsonToken.Subject,
		Permissions: permissions,
		Expiration:  jsonToken.Expiration,
	}, nil
}

func verifyTokenFromCtx(ctx context.Context) (*JSONToken, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no headers found on request")
	}
	authHeader, ok := md["authorization"]
	if !ok || len(authHeader) < 1 {
		return nil, status.Errorf(codes.Unauthenticated, "no Authorization header found on request")
	}
	token := strings.TrimPrefix(authHeader[0], "Bearer ")
	jsonToken, err := verifyToken(token)
	if err != nil {
		return nil, status.Error(
			codes.Unauthenticated, "invalid token")
	}
	return jsonToken, nil
}

const minPasswordLen = 8
const maxPasswordLen = 32

var PasswordRequirementsMsg = fmt.Sprintf(
	"password must have between %d and %d letters, digits and special characters "+
		"of which at least 1 uppercase letter, 1 digit and 1 special character",
	minPasswordLen,
	maxPasswordLen,
)

func IsStrongPassword(password string) error {
	err := errors.New(PasswordRequirementsMsg)
	if len(password) < minPasswordLen || len(password) > maxPasswordLen {
		return err
	}
	var hasUpper bool
	var hasDigit bool
	var hasSpecial bool
	for _, ch := range password {
		switch {
		case unicode.IsUpper(ch):
			hasUpper = true
		case unicode.IsLower(ch):
		case unicode.IsDigit(ch):
			hasDigit = true
		case unicode.IsPunct(ch) || unicode.IsSymbol(ch):
			hasSpecial = true
		default:
			return err
		}
	}
	if !hasUpper || !hasDigit || !hasSpecial {
		return err
	}
	return nil
}

const loginMethod = "/immudb.schema.ImmuService/Login"

var methodsWithoutAuth = map[string]bool{
	"/immudb.schema.ImmuService/CurrentRoot": true,
	"/immudb.schema.ImmuService/Health":      true,
	loginMethod:                              true,
}

func HasAuth(method string) bool {
	_, noAuth := methodsWithoutAuth[method]
	return !noAuth
}

const ClientIDMetadataKey = "client_id"
const ClientIDMetadataValueAdmin = "immuadmin"

func IsAdminClient(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	clientIDMD := md[ClientIDMetadataKey]
	return len(clientIDMD) > 0 && clientIDMD[0] == ClientIDMetadataValueAdmin
}

var IsAdminUser func(ctx context.Context, username []byte) (bool, error)
var AdminUserExists func(ctx context.Context) (bool, error)
var CreateAdminUser func(ctx context.Context) (string, string, error)

var ErrServerAuthDisabled = status.Error(
	codes.Unavailable, "authentication is disabled on server")

type ErrFirstAdminLogin struct {
	message string
}

func (e *ErrFirstAdminLogin) Error() string {
	return e.message
}

func (e *ErrFirstAdminLogin) With(username string, password string) *ErrFirstAdminLogin {
	e.message = fmt.Sprintf(
		"FirstAdminLogin\n---\nusername: %s\npassword: %s\n---\n",
		username,
		password,
	)
	return e
}

func (e *ErrFirstAdminLogin) Matches(err error) (string, bool) {
	errMsg := err.Error()
	grpcErrPieces := strings.Split(errMsg, "desc =")
	if len(grpcErrPieces) > 1 {
		errMsg = strings.TrimSpace(strings.Join(grpcErrPieces[1:], ""))
	}
	return strings.TrimPrefix(errMsg, "FirstAdminLogin"),
		strings.Index(errMsg, "FirstAdminLogin") == 0
}

func createAdminUserAndMsg(ctx context.Context) (*ErrFirstAdminLogin, error) {
	username, plainPassword, err := CreateAdminUser(ctx)
	if err == nil {
		return (&ErrFirstAdminLogin{}).With(username, plainPassword), nil
	}
	return nil, err
}

func checkAuth(ctx context.Context, method string, req interface{}) error {
	isAdminCLI := IsAdminClient(ctx)
	if !AuthEnabled || isAdminCLI {
		if !isLocalClient(ctx) {
			var errMsg string
			if isAdminCLI {
				errMsg = "server does not accept admin commands from remote clients"
			} else {
				errMsg =
					"server has authentication disabled: only local connections are accepted"
			}
			return status.Errorf(codes.PermissionDenied, errMsg)
		}
	}
	if !AuthEnabled && !isAdminCLI {
		return nil
	}
	if method == loginMethod && isAdminCLI {
		lReq, ok := req.(*schema.LoginRequest)
		// if it's the very first admin login attempt, generate admin user and password
		if ok && string(lReq.GetUser()) == AdminUsername && len(lReq.GetPassword()) == 0 {
			adminUserExists, err := AdminUserExists(ctx)
			if err != nil {
				return fmt.Errorf("error determining if admin user exists: %v", err)
			}
			if !adminUserExists {
				firstAdminCallMsg, err := createAdminUserAndMsg(ctx)
				if err != nil {
					return err
				}
				return firstAdminCallMsg
			}
		}
		// do not allow users other than admin to login from immuadmin CLI
		isAdmin, err := IsAdminUser(ctx, lReq.GetUser())
		if err != nil {
			return err
		}
		if !isAdmin {
			return status.Errorf(codes.PermissionDenied, "permission denied")
		}
	}
	if HasAuth(method) {
		jsonToken, err := verifyTokenFromCtx(ctx)
		if err != nil {
			return err
		}
		if !HasPermissionForMethod(jsonToken.Permissions, method) {
			return status.Errorf(codes.PermissionDenied, "not enough permissions")
		}
	}
	return nil
}
