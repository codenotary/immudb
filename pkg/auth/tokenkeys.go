/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

func generateKeys(Username string) error {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return fmt.Errorf(
			"error generating public and private key pair for user %s: %v",
			Username, err)
	}
	tokenKeyPairs.Lock()
	defer tokenKeyPairs.Unlock()
	tokenKeyPairs.keysPerUser[Username] =
		&tokenKeyPair{publicKey, privateKey, time.Now()}
	return nil
}

func getTokenForUser(Username string) (*tokenKeyPair, bool) {
	tokenKeyPairs.RLock()
	defer tokenKeyPairs.RUnlock()
	kp, ok := tokenKeyPairs.keysPerUser[Username]
	return kp, ok
}

func updateLastTokenGeneratedAt(Username string) {
	tokenKeyPairs.Lock()
	defer tokenKeyPairs.Unlock()
	tokenKeyPairs.keysPerUser[Username].lastTokenGeneratedAt = time.Now()
}

func evictOldTokenKeyPairs() {
	tokenKeyPairs.Lock()
	defer tokenKeyPairs.Unlock()
	// 1 public key = 32B, 1 private key = 64B =>
	// 10_000 key pairs = (32 + 64) * 10_000 = 960_000B which is close to 1MB
	// if storing keys requires less memory than that, skip eviction
	if len(tokenKeyPairs.keysPerUser) < 10_000 {
		return
	}

	now := time.Now()
	if now.Before(tokenKeyPairs.lastEvictedAt.Add(tokenKeyPairs.minEvictInterval)) {
		return
	}
	for k, v := range tokenKeyPairs.keysPerUser {
		// - keys are used to generate tokens during login (and to verify them during any call with auth)
		// - if no token was generated with a key during the last 3 days, the user would have to login
		//   again anyway (tokens expire in a much shorter time than that), so we just evict the key
		//   (if user logins again, a new pair will be generated and used from that point on)
		if now.Before(v.lastTokenGeneratedAt.Add(3 * 24 * time.Hour)) {
			continue
		}
		delete(tokenKeyPairs.keysPerUser, k)
	}
	tokenKeyPairs.lastEvictedAt = now
}

// DropTokenKeys removes the token keys from the cache, hence invalidating
// any token that was generated with those keys
func DropTokenKeys(username string) bool {
	tokenKeyPairs.Lock()
	defer tokenKeyPairs.Unlock()
	_, ok := tokenKeyPairs.keysPerUser[username]
	if ok {
		delete(tokenKeyPairs.keysPerUser, username)
	}
	return ok
}

// DropTokenKeysForCtx removes the token keys from the cache for the username of
// the token that resides in the provided context
func DropTokenKeysForCtx(ctx context.Context) (bool, error) {
	jsonToken, err := verifyTokenFromCtx(ctx)
	if err != nil {
		return false, err
	}
	return DropTokenKeys(jsonToken.Username), nil
}

// GetLoggedInUser gets userdata from context
func GetLoggedInUser(ctx context.Context) (*JSONToken, error) {
	return verifyTokenFromCtx(ctx)
}
