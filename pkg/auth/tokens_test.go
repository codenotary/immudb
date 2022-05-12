/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestUUID(t *testing.T) {
	uuid := NewUUID()
	if len(uuid.Bytes()) == 0 {
		t.Errorf("NewUUID, error generating uuid")
	}

	strUUID := NewStringUUID()
	if len(strUUID) == 0 {
		t.Errorf("NewStringUUID, error generating uuid")
	}
}
func TestToken(t *testing.T) {
	u := User{
		Username: "immudb",
		Active:   true,
	}
	token, err := GenerateToken(u, 2, 60)
	if err != nil {
		t.Errorf("Error GenerateToken %s", err)
	}
	if len(token) == 0 {
		t.Errorf("Error GenerateToken token length equal to zero")
	}

	jToken, err := verifyToken(token)
	if err != nil {
		t.Errorf("Error verifyToken %s", err)
	}
	if jToken.Username != u.Username {
		t.Errorf("Token username error %s", jToken.Username)
	}
	if jToken.DatabaseIndex != 2 {
		t.Errorf("Token DatabaseIndex error %d", jToken.DatabaseIndex)
	}
	wrongToken := strings.Replace(token, ".", "", 2)
	_, err = verifyToken(wrongToken)
	if err == nil {
		t.Errorf("verifyToken, failed to catch token error %s", err)
	}
}

func TestVerifyFromCtx(t *testing.T) {
	u := User{
		Username: "immudb",
		Active:   true,
	}
	token, err := GenerateToken(u, 2, 60)
	if err != nil {
		t.Errorf("Error GenerateToken %s", err)
	}
	ctx := context.Background()
	_, err = verifyTokenFromCtx(ctx)
	if err == nil {
		t.Errorf("Error verifyTokenFromCtx on empty context")
	}
	m := make(map[string][]string)
	m["authorization"] = []string{token}
	newCtx := metadata.NewIncomingContext(ctx, m)
	js, err := verifyTokenFromCtx(newCtx)
	if err != nil {
		t.Errorf("Error verifyTokenFromCtx %s", err)
	}
	if js.Username != u.Username {
		t.Errorf("Token username error %s", js.Username)
	}
	if js.DatabaseIndex != 2 {
		t.Errorf("Token DatabaseIndex error %d", js.DatabaseIndex)
	}

	wrongToken := strings.Replace(token, ".", "", 2)
	m = make(map[string][]string)
	m["authorization"] = []string{wrongToken}
	newCtx = metadata.NewIncomingContext(ctx, m)
	_, err = verifyTokenFromCtx(newCtx)
	if err == nil {
		t.Errorf("Error verifyTokenFromCtx wrong token")
	}

	m = make(map[string][]string)
	m["authorization"] = []string{}
	newCtx = metadata.NewIncomingContext(ctx, m)
	_, err = verifyTokenFromCtx(newCtx)
	if err == nil {
		t.Errorf("Error verifyTokenFromCtx empty token")
	}
}
