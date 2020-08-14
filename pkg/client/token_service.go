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

package client

import (
	"encoding/binary"
	"errors"
	"strings"
)

type TokenService interface {
	SetToken(database string, token string) error
	WithHds(hds HomedirService) TokenService
	WithTokenFileName(tfn string) TokenService
	IsTokenPresent() (bool, error)
	DeleteToken() error
	GetToken() (string, error)
	GetDatabase() (string, error)
}

type tokenService struct {
	tokenFileName string
	hds           HomedirService
}

//NewTokenService ...
func NewTokenService() TokenService {
	return tokenService{}
}

func (ts tokenService) GetToken() (string, error) {
	_, token, err := ts.parseContent()
	if err != nil {
		return "", err
	}
	return token, nil
}

//SetToken ...
func (ts tokenService) SetToken(database string, token string) error {
	return ts.hds.WriteFileToUserHomeDir(BuildToken(database, token), ts.tokenFileName)
}

func BuildToken(database string, token string) []byte {
	dbsl := uint64(len(database))
	dbnl := len(database)
	tl := len(token)
	lendbs := binary.Size(dbsl)
	var cnt = make([]byte, lendbs+dbnl+tl)
	binary.BigEndian.PutUint64(cnt, dbsl)
	copy(cnt[lendbs:], database)
	copy(cnt[lendbs+dbnl:], token)
	return cnt
}

func (ts tokenService) DeleteToken() error {
	return ts.hds.DeleteFileFromUserHomeDir(ts.tokenFileName)
}

//IsTokenPresent ...
func (ts tokenService) IsTokenPresent() (bool, error) {
	return ts.hds.FileExistsInUserHomeDir(ts.tokenFileName)
}

func (ts tokenService) GetDatabase() (string, error) {
	dbname, _, err := ts.parseContent()
	return dbname, err
}

func (ts tokenService) parseContent() (string, string, error) {
	content, err := ts.hds.ReadFileFromUserHomeDir(ts.tokenFileName)
	if err != nil {
		return "", "", err
	}
	if len(content) <= 8 {
		return "", "", errors.New("token content not present")
	}
	// token prefix is hardcoded into library. Please modify in case of changes in paseto library
	if strings.HasPrefix(content, "v2.public.") {
		return "", "", errors.New("old token format. Please remove old token located in your default home dir")
	}
	databasel := make([]byte, 8)
	copy(databasel, content[:8])
	databaselUint64 := binary.BigEndian.Uint64(databasel)
	databasename := make([]byte, int(databaselUint64))
	copy(databasename, content[8:8+int(databaselUint64)])

	token := make([]byte, len(content)-8-int(databaselUint64))
	copy(token, content[8+int(databaselUint64):])

	return string(databasename), string(token), nil
}

// WithHds ...
func (ts tokenService) WithHds(hds HomedirService) TokenService {
	ts.hds = hds
	return ts
}

// WithTokenFileName ...
func (ts tokenService) WithTokenFileName(tfn string) TokenService {
	ts.tokenFileName = tfn
	return ts
}
