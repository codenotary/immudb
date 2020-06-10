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
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"
	"unicode"

	"golang.org/x/crypto/bcrypt"
)

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

func DecodeBase64Password(passwordBase64 string) string {
	password := strings.TrimSpace(passwordBase64)
	if password != "" {
		passwordBytes, err := base64.StdEncoding.DecodeString(passwordBase64)
		if err == nil {
			password = string(passwordBytes)
		}
	}
	return password
}
