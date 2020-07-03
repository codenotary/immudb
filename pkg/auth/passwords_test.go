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
	"encoding/base64"
	"strings"
	"testing"
)

func TestIsStrongPassword(t *testing.T) {
	weakPass := "pass"
	if err := IsStrongPassword(weakPass); err == nil {
		t.Errorf("IsStrongPassword failed to detect week password")
	}
	weakPass = "1~password"
	if err := IsStrongPassword(weakPass); err == nil {
		t.Errorf("IsStrongPassword failed to detect week password")
	}
	weakPass = "1~Password"
	if err := IsStrongPassword(weakPass); err != nil {
		t.Errorf("IsStrongPassword detected wrong week password")
	}
	weakPass = "1~Password\n"
	if err := IsStrongPassword(weakPass); err == nil {
		t.Errorf("IsStrongPassword failed to detect non allowed character")
	}
}
func TestDecodeBase64Password(t *testing.T) {
	pass := "pass"
	_, err := DecodeBase64Password(pass)
	if err != err {
		t.Errorf("DecodeBase64Password error detected wrong password")
	}
	pass = "enc:" + base64.StdEncoding.EncodeToString([]byte("password"))
	decodedPass, err := DecodeBase64Password(pass)
	if err != err {
		t.Errorf("DecodeBase64Password %s", err)
	}
	if decodedPass != "password" {
		t.Errorf("DecodeBase64Password error decoding password")
	}

	_, err = DecodeBase64Password(strings.TrimSuffix(pass, "="))
	if err != err {
		t.Errorf("DecodeBase64Password %s", err)
	}

}
