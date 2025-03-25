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
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestDropTokenKeys(t *testing.T) {
	if err := generateKeys("CharlesDickens"); err != nil {
		t.Errorf("error generating keys %s", err)
	}
	if !DropTokenKeys("CharlesDickens") {
		t.Errorf("error drop token keys")
	}
	evictOldTokenKeyPairs()
	public, _ := hex.DecodeString("93bced46788771711821e9aa6e89b65c8080436ee1f9f24c140613f47c89b435")
	private, _ := hex.DecodeString("a11110e174b875ac7d50ff26c444710e3e3ef49249e891638a64b53ba41bd1b393bced46788771711821e9aa6e89b65c8080436ee1f9f24c140613f47c89b435")
	tm := time.Unix(1593767032, 0)
	keyPair := &tokenKeyPair{
		publicKey:            public,
		privateKey:           private,
		lastTokenGeneratedAt: tm,
	}
	for i := 0; i < 11_000; i++ {
		tokenKeyPairs.keysPerUser["CharlesDickens"+strconv.Itoa(i)] = keyPair
	}
	evictOldTokenKeyPairs()
}

func TestDropTokenKeysForCtx(t *testing.T) {
	u := User{
		Username: "copperfield",
		Active:   true,
	}
	generateKeys("copperfield")
	token, err := GenerateToken(u, 2, 60)
	require.NoError(t, err)
	m := make(map[string][]string)
	m["authorization"] = []string{token}
	ctx := metadata.NewIncomingContext(context.Background(), m)
	js, err := GetLoggedInUser(ctx)
	require.NoError(t, err)
	if js.Username != u.Username {
		t.Errorf("Error GetLoggedInUser usernames do not match")
	}
	_, err = DropTokenKeysForCtx(ctx)
	require.NoError(t, err)
}
