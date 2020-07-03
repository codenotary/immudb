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
	"strconv"
	"testing"
	"time"

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
	token, err := GenerateToken(u, 2)
	if err != nil {
		t.Errorf("Error GenerateToken %s", err)
	}
	m := make(map[string][]string)
	m["authorization"] = []string{token}
	ctx := metadata.NewIncomingContext(context.Background(), m)
	js, err := GetLoggedInUser(ctx)
	if err != nil {
		t.Errorf("Error GetLoggedInUser %s", err)
	}
	if js.Username != u.Username {
		t.Errorf("Error GetLoggedInUser usernames do not match")
	}
	_, err = DropTokenKeysForCtx(ctx)
	if err != nil {
		t.Errorf("Error DropTokenKeysForCtx %s", err)
	}
}
