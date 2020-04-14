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
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/o1egl/paseto"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/ed25519"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// generates a random ASCII string with at least one digit and one special character
func generatePassword() string {
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

// NOTE: bcrypt.MinCost is 4
const passwordHashCostDefault = 6
const passwordHashCostHigh = bcrypt.DefaultCost

func hashAndSaltPassword(plainPassword string, highCost bool) ([]byte, error) {
	hashCost := passwordHashCostDefault
	if highCost {
		hashCost = passwordHashCostHigh
	}
	hashedPasswordBytes, err := bcrypt.GenerateFromPassword([]byte(plainPassword), hashCost)
	if err != nil {
		return nil, fmt.Errorf("error hashing password: %v", err)
	}
	return hashedPasswordBytes, nil
}

func comparePasswords(hashedPassword []byte, plainPassword []byte) error {
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
		return fmt.Errorf("error generating public and private keys: %v", err)
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

const footer = "CodeNotary"
const tokenValidity = 1 * time.Hour

// GenerateToken ...
func GenerateToken(username string) (string, error) {
	now := time.Now()

	token, err := pasetoV2.Sign(
		privateKey,
		paseto.JSONToken{
			Expiration: now.Add(tokenValidity),
			Subject:    username,
		},
		footer)
	if err != nil {
		return "", fmt.Errorf("error generating token: %v", err)
	}

	return token, nil
}

// JSONToken ...
type JSONToken struct {
	Username   string
	Expiration time.Time
}

func verifyToken(token string) (*JSONToken, error) {
	var jsonToken paseto.JSONToken
	var footer string
	if err := pasetoV2.Verify(token, publicKey, &jsonToken, &footer); err != nil {
		return nil, err
	}
	if err := jsonToken.Validate(); err != nil {
		return nil, err
	}
	return &JSONToken{Username: jsonToken.Subject, Expiration: jsonToken.Expiration}, nil
}

func verifyTokenFromCtx(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Internal, "no headers found on request")
	}
	authHeader, ok := md["authorization"]
	if !ok || len(authHeader) < 1 {
		return status.Errorf(codes.Unauthenticated, "no Authorization header found on request")
	}
	token := strings.TrimPrefix(authHeader[0], "Bearer ")
	_, err := verifyToken(token)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "invalid token %s", token)
	}
	return nil
}

var methodsWithoutAuth = map[string]bool{
	"/immudb.schema.ImmuService/CurrentRoot": true,
	"/immudb.schema.ImmuService/Health":      true,
	"/immudb.schema.ImmuService/Login":       true,
}

func HasAuth(method string) bool {
	_, noAuth := methodsWithoutAuth[method]
	return !noAuth
}
