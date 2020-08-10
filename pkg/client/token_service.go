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
)

type Token_service interface {
	SetToken(database string, token string) error
	WithHds(hds HomedirService) Token_service
	WithTokenFileName(tfn string) Token_service
	IsTokenPresent() (bool, error)
	DeleteToken() error
	GetToken() (string, error)
	GetDatabase() (string, error)
}

type token_service struct {
	tokenFileName string
	hds           HomedirService
}

//NewTokenService ...
func NewTokenService() Token_service {
	return token_service{}
}

func (ts token_service) GetToken() (string, error) {
	_, token, err := ts.parseContent()
	if err != nil {
		return "", err
	}
	return token, nil
}

//SetToken ...
func (ts token_service) SetToken(database string, token string) error {
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

func (ts token_service) DeleteToken() error {
	return ts.hds.DeleteFileFromUserHomeDir(ts.tokenFileName)
}

//IsTokenPresent ...
func (ts token_service) IsTokenPresent() (bool, error) {
	return ts.hds.FileExistsInUserHomeDir(ts.tokenFileName)
}

func (ts token_service) GetDatabase() (string, error) {
	dbname, _, err := ts.parseContent()
	return dbname, err
}

func (ts token_service) parseContent() (string, string, error) {
	content, err := ts.hds.ReadFileFromUserHomeDir(ts.tokenFileName)
	if err != nil {
		return "", "", err
	}
	if len(content) <= 8 {
		return "", "", errors.New("token content not present")
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
func (ts token_service) WithHds(hds HomedirService) Token_service {
	ts.hds = hds
	return ts
}

// WithTokenFileName ...
func (ts token_service) WithTokenFileName(tfn string) Token_service {
	ts.tokenFileName = tfn
	return ts
}
