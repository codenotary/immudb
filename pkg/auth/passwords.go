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
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"golang.org/x/crypto/bcrypt"
)

// HashAndSaltPassword hashes and salts the provided password
func HashAndSaltPassword(plainPassword []byte) ([]byte, error) {
	hashedPasswordBytes, err := bcrypt.GenerateFromPassword(plainPassword, bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("error hashing password: %v", err)
	}
	return hashedPasswordBytes, nil
}

// ComparePasswords compares the provided plainPassword against the provided hashed password
func ComparePasswords(hashedPassword []byte, plainPassword []byte) error {
	return bcrypt.CompareHashAndPassword(hashedPassword, plainPassword)
}

const (
	minPasswordLen = 8
	maxPasswordLen = 32
)

// PasswordRequirementsMsg message used to inform the user about password strength requirements
var PasswordRequirementsMsg = fmt.Sprintf(
	"password must have between %d and %d letters, digits and special characters "+
		"of which at least 1 uppercase letter, 1 digit and 1 special character",
	minPasswordLen,
	maxPasswordLen,
)

// IsStrongPassword checks if the provided password meets the strength requirements
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

// DecodeBase64Password decodes the provided base64-encoded password if it has the
// "enc:" prefix or returns it with leading and trailing space trimmed otherwise
func DecodeBase64Password(passwordBase64 string) (string, error) {
	password := strings.TrimSpace(passwordBase64)
	prefix := "enc:"
	if password != "" && strings.HasPrefix(password, prefix) {
		passwordNoPrefix := passwordBase64[4:]
		passwordBytes, err := base64.StdEncoding.DecodeString(passwordNoPrefix)
		if err != nil {
			return passwordBase64, fmt.Errorf(
				"error decoding password from base64 string %s: %v", passwordNoPrefix, err)
		}
		password = string(passwordBytes)
	}
	return password, nil
}
