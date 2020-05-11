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

package server

import (
	"bytes"
	"context"
	"errors"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var deletedFlag = []byte("DELETED")

func isFlaggedAsDeleted(user *schema.Item) bool {
	return bytes.Equal(user.GetValue(), deletedFlag)
}

func (s *ImmuServer) adminUserExists(ctx context.Context) bool {
	item, err := s.SysStore.Get(schema.Key{Key: []byte(auth.AdminUsername)})
	if err != nil && err != store.ErrKeyNotFound {
		s.Logger.Errorf("error getting admin user: %v", err)
		return false
	}
	return err == nil && !isFlaggedAsDeleted(item)
}

func (s *ImmuServer) createAdminUser(ctx context.Context) (string, string, error) {
	if s.adminUserExists(ctx) {
		return "", "", errors.New("admin user already exists")
	}
	u := auth.User{Username: auth.AdminUsername, Admin: true}
	plainPass, err := u.GenerateAndSetPassword()
	if err != nil {
		s.Logger.Errorf("error generating password for admin user: %v", err)
	}
	kv := schema.KeyValue{
		Key:   []byte(u.Username),
		Value: u.HashedPassword,
	}
	if _, err := s.SysStore.Set(kv); err != nil {
		s.Logger.Errorf("error creating admin user: %v", err)
		return "", "", err
	}
	return u.Username, plainPass, nil
}

func (s *ImmuServer) CreateUser(ctx context.Context, r *schema.CreateUserRequest) (*schema.CreateUserResponse, error) {
	item, err := s.SysStore.Get(schema.Key{Key: r.GetUser()})
	if err != nil && err != store.ErrKeyNotFound {
		s.Logger.Errorf("error checking if user already exists: %v", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	if err == nil && !isFlaggedAsDeleted(item) {
		return nil, status.Errorf(codes.AlreadyExists, "username already exists")
	}
	if err = auth.IsStrongPassword(string(r.GetPassword())); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	hashedPassword, err := auth.HashAndSaltPassword(string(r.GetPassword()))
	if err != nil {
		return nil, err
	}
	kv := schema.KeyValue{
		Key:   r.GetUser(),
		Value: hashedPassword,
	}
	if _, err := s.SysStore.Set(kv); err != nil {
		s.Logger.Errorf("error persisting new user: %v", err)
		return nil, err
	}
	return &schema.CreateUserResponse{
		User: r.GetUser(),
	}, nil
}

func (s *ImmuServer) ChangePassword(ctx context.Context, r *schema.ChangePasswordRequest) (*empty.Empty, error) {
	e := new(empty.Empty)
	item, err := s.SysStore.Get(schema.Key{Key: r.GetUser()})
	if err != nil {
		s.Logger.Errorf("error getting user: %v", err)
		return e, err
	}
	if isFlaggedAsDeleted(item) {
		return e, status.Errorf(codes.NotFound, "error changing password: user was already deleted")
	}
	if err = auth.ComparePasswords(item.GetValue(), r.GetOldPassword()); err != nil {
		return e, status.Errorf(codes.PermissionDenied, "old password is incorrect")
	}
	newPass := string(r.GetNewPassword())
	if err = auth.IsStrongPassword(newPass); err != nil {
		return e, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	hashedPassword, err := auth.HashAndSaltPassword(newPass)
	if err != nil {
		return e, status.Errorf(codes.Internal, "%v", err)
	}
	kv := schema.KeyValue{
		Key:   r.GetUser(),
		Value: hashedPassword,
	}
	if _, err := s.SysStore.Set(kv); err != nil {
		s.Logger.Errorf("error persisting changed password: %v", err)
		return e, err
	}
	return e, nil
}

func (s *ImmuServer) DeleteUser(ctx context.Context, r *schema.DeleteUserRequest) (*empty.Empty, error) {
	e := new(empty.Empty)
	item, err := s.SysStore.Get(schema.Key{Key: r.GetUser()})
	if err != nil {
		s.Logger.Errorf("error getting user: %v", err)
		return e, err
	}
	if isFlaggedAsDeleted(item) {
		return e, status.Errorf(codes.NotFound, "user not found")
	}
	kv := schema.KeyValue{
		Key:   r.GetUser(),
		Value: deletedFlag,
	}
	if _, err := s.SysStore.Set(kv); err != nil {
		s.Logger.Errorf("error deleting user: %v", err)
		return e, err
	}
	return e, nil
}
