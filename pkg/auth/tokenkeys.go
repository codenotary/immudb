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
	"crypto/ed25519"
	"fmt"
	"sync"
	"time"
)

type tokenKeyPair struct {
	publicKey            ed25519.PublicKey
	privateKey           ed25519.PrivateKey
	lastTokenGeneratedAt time.Time
}

var tokenKeyPairs = struct {
	keysPerUser      map[string]*tokenKeyPair
	lastEvictedAt    time.Time
	minEvictInterval time.Duration
	sync.RWMutex
}{
	keysPerUser:      map[string]*tokenKeyPair{},
	lastEvictedAt:    time.Unix(0, 0),
	minEvictInterval: 1 * time.Hour,
}

func generateKeys(username string) error {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return fmt.Errorf(
			"error generating public and private key pair for user %s: %v",
			username, err)
	}
	tokenKeyPairs.Lock()
	defer tokenKeyPairs.Unlock()
	tokenKeyPairs.keysPerUser[username] =
		&tokenKeyPair{publicKey, privateKey, time.Now()}
	return nil
}

func updateLastTokenGeneratedAt(username string) {
	tokenKeyPairs.Lock()
	defer tokenKeyPairs.Unlock()
	tokenKeyPairs.keysPerUser[username].lastTokenGeneratedAt = time.Now()
}

func evictOldTokenKeyPairs() {
	tokenKeyPairs.Lock()
	defer tokenKeyPairs.Unlock()
	now := time.Now()
	if now.Before(tokenKeyPairs.lastEvictedAt.Add(tokenKeyPairs.minEvictInterval)) {
		return
	}
	for k, v := range tokenKeyPairs.keysPerUser {
		if now.Before(v.lastTokenGeneratedAt.Add(2 * tokenValidity)) {
			continue
		}
		delete(tokenKeyPairs.keysPerUser, k)
	}
	tokenKeyPairs.lastEvictedAt = now
}

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
