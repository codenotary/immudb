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
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
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
var publicKey ed25519.PublicKey
var privateKey ed25519.PrivateKey

// GenerateKeys ...
func GenerateKeys() error {
	var err error
	publicKey, privateKey, err = ed25519.GenerateKey(nil)
	if err != nil {
		return fmt.Errorf("error generating public and private keys (used for signing and verifying tokens): %v", err)
	}
	return nil
}

// GenerateOrLoadKeys ...
func GenerateOrLoadKeys() error {
	publicKeyFileName := "immudb_public_key"
	_, errPublic := os.Stat(publicKeyFileName)
	privateKeyFileName := "immudb_private_key"
	_, errPrivate := os.Stat(privateKeyFileName)

	bothExist := !os.IsNotExist(errPublic) && !os.IsNotExist(errPrivate)
	if !bothExist {
		if err := GenerateKeys(); err != nil {
			return fmt.Errorf("error generating public and private keys: %v", err)
		}
		if err := writeKeyToFile([]byte(publicKey), publicKeyFileName); err != nil {
			return fmt.Errorf("error writing public key to file %s: %v", publicKeyFileName, err)
		}
		if err := writeKeyToFile([]byte(privateKey), privateKeyFileName); err != nil {
			return fmt.Errorf("error writing private key to file %s: %v", privateKeyFileName, err)
		}
		return nil
	}

	publicKeyBytes, err := readKeyFromFile(publicKeyFileName)
	if err != nil {
		return fmt.Errorf("error loading public key from file %s: %v", privateKeyFileName, err)
	}
	privateKeyBytes, err := readKeyFromFile(privateKeyFileName)
	if err != nil {
		return fmt.Errorf("error loading private key from file %s: %v", privateKeyFileName, err)
	}
	publicKey = ed25519.PublicKey(publicKeyBytes)
	privateKey = ed25519.PrivateKey(privateKeyBytes)

	return nil
}

func writeKeyToFile(key []byte, fileName string) error {
	keyHex := make([]byte, hex.EncodedLen(len(key)))
	hex.Encode(keyHex, key)
	return ioutil.WriteFile(fileName, keyHex, 0644)
}

func readKeyFromFile(fileName string) ([]byte, error) {
	keyBytesRead, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("error reading from file %s: %v", fileName, err)
	}
	keyBytes := make([]byte, hex.DecodedLen(len(keyBytesRead)))
	_, err = hex.Decode(keyBytes, keyBytesRead)
	if err != nil {
		return nil, fmt.Errorf("error hex decoding key: %v", err)
	}
	return keyBytes, nil
}

const footer = "immudb"
const tokenValidity = 1 * time.Hour

// GenerateToken ...
func GenerateToken(user User) (string, error) {
	if privateKey == nil || len(privateKey) == 0 {
		if err := GenerateKeys(); err != nil {
			return "", err
		}
	}
	now := time.Now()
	jsonToken := paseto.JSONToken{
		Expiration: now.Add(tokenValidity),
		Subject:    user.Username,
	}
	jsonToken.Set("admin", strconv.FormatBool(user.Admin))
	token, err := pasetoV2.Sign(privateKey, jsonToken, footer)
	if err != nil {
		return "", fmt.Errorf("error generating token: %v", err)
	}

	return token, nil
}

// JSONToken ...
type JSONToken struct {
	Username   string
	Admin      bool
	Expiration time.Time
}

func verifyToken(token string) (*JSONToken, error) {
	if publicKey == nil || len(publicKey) == 0 {
		if err := GenerateKeys(); err != nil {
			return nil, err
		}
	}
	var jsonToken paseto.JSONToken
	var footer string
	if err := pasetoV2.Verify(token, publicKey, &jsonToken, &footer); err != nil {
		return nil, err
	}
	if err := jsonToken.Validate(); err != nil {
		return nil, err
	}
	admin, _ := strconv.ParseBool(jsonToken.Get("admin"))
	return &JSONToken{
		Username:   jsonToken.Subject,
		Admin:      admin,
		Expiration: jsonToken.Expiration,
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
		return nil, status.Errorf(codes.Unauthenticated, "invalid token %s", token)
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

var empty struct{}
var methodsForAdmin = map[string]struct{}{
	"/immudb.schema.ImmuService/CreateUser":     empty,
	"/immudb.schema.ImmuService/ChangePassword": empty,
	"/immudb.schema.ImmuService/DeleteUser":     empty,
	"/immudb.schema.ImmuService/Backup":         empty,
	"/immudb.schema.ImmuService/Restore":        empty,
}

const ClientIDMetadataKey = "client_id"
const ClientIDMetadataValueAdmin = "immuadmin"

func isAdmin(method string) bool {
	_, ok := methodsForAdmin[method]
	return ok
}
func IsAdminClient(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	clientIDMD := md[ClientIDMetadataKey]
	return len(clientIDMD) > 0 && clientIDMD[0] == ClientIDMetadataValueAdmin
}

var AdminUserExists func(ctx context.Context) bool
var CreateAdminUser func(ctx context.Context) (string, string, error)

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
	if isAdminCLI && !AuthEnabled {
		if !isLocalClient(ctx) {
			return status.Errorf(
				codes.PermissionDenied,
				"server has authentication disabled: only local connections are accepted")
		}
	}
	isAuthEnabled := AuthEnabled || isAdminCLI
	if method == loginMethod && isAuthEnabled && isAdminCLI {
		lReq, ok := req.(*schema.LoginRequest)
		// if it's the very first admin login attempt, generate admin user and password
		if ok && string(lReq.GetUser()) == AdminUsername &&
			len(lReq.GetPassword()) == 0 && !AdminUserExists(ctx) {
			firstAdminCallMsg, err2 := createAdminUserAndMsg(ctx)
			if err2 != nil {
				return err2
			}
			return firstAdminCallMsg
		}
		// do not allow users other than admin to login from immuadmin CLI
		if string(lReq.GetUser()) != AdminUsername {
			return status.Errorf(codes.PermissionDenied, "permission denied")
		}
	}
	if isAuthEnabled && HasAuth(method) {
		jsonToken, err := verifyTokenFromCtx(ctx)
		if err != nil {
			return err
		}
		if isAdmin(method) || isAdminCLI {
			if !jsonToken.Admin {
				return status.Errorf(codes.PermissionDenied, "permission denied")
			}
			if !isLocalClient(ctx) {
				return status.Errorf(
					codes.PermissionDenied,
					"server does not accept admin commands from remote clients")
			}
		}
	}
	return nil
}
