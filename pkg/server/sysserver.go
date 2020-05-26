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
	"fmt"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/store/sysstore"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var deletedFlag = []byte("DELETED")

func isFlaggedAsDeleted(user *schema.Item) bool {
	return bytes.Equal(user.GetValue(), deletedFlag)
}

func (s *ImmuServer) getUsersLatestVersions(username []byte) ([]*schema.Item, error) {
	prefix := sysstore.UserPrefix()
	if username != nil {
		prefix = sysstore.AddUserPrefix(username)
	}
	itemList, err := s.SysStore.Scan(schema.ScanOptions{
		Prefix: prefix,
	})
	if err != nil {
		s.Logger.Errorf("error checking if admin user exists: %v", err)
		return nil, err
	}
	if len(itemList.Items) == 0 {
		return nil, nil
	}
	latestItems := map[string]*schema.Item{}
	for _, item := range itemList.GetItems() {
		u := string(auth.TrimPermissionSuffix(item.GetKey()))
		prev, ok := latestItems[u]
		if !ok || prev.GetIndex() < item.GetIndex() {
			latestItems[u] = item
		}
	}
	items := make([]*schema.Item, 0, len(latestItems))
	for _, item := range latestItems {
		items = append(items, item)
	}
	return items, nil
}

func (s *ImmuServer) adminUserExists(ctx context.Context) (bool, error) {
	items, err := s.getUsersLatestVersions(nil)
	if err != nil || len(items) <= 0 {
		return false, err
	}
	for _, item := range items {
		if !isFlaggedAsDeleted(item) &&
			auth.HasPermissionSuffix(item.GetKey(), auth.PermissionAdmin) {
			return true, nil
		}
	}
	return false, nil
}
func (s *ImmuServer) isAdminUser(ctx context.Context, username []byte) (bool, error) {
	items, err := s.getUsersLatestVersions(username)
	if err != nil || len(items) <= 0 {
		return false, err
	}
	for _, item := range items {
		if !isFlaggedAsDeleted(item) &&
			auth.HasPermissionSuffix(item.GetKey(), auth.PermissionAdmin) {
			return true, nil
		}
	}
	return false, nil
}

func (s *ImmuServer) createAdminUser(ctx context.Context) (string, string, error) {
	exists, err := s.adminUserExists(ctx)
	if err != nil {
		return "", "", fmt.Errorf(
			"error determining if admin user exists: %v", err)
	}
	if exists {
		return "", "", errors.New("admin user already exists")
	}
	u := auth.User{Username: auth.AdminUsername}
	plainPass, err := u.GenerateAndSetPassword()
	if err != nil {
		s.Logger.Errorf("error generating password for admin user: %v", err)
	}
	kv := schema.KeyValue{
		Key: auth.AddPermissionSuffix(
			sysstore.AddUserPrefix([]byte(u.Username)), auth.PermissionAdmin),
		Value: u.HashedPassword,
	}
	if _, err := s.SysStore.Set(kv); err != nil {
		s.Logger.Errorf("error creating admin user: %v", err)
		return "", "", err
	}
	return u.Username, plainPass, nil
}

func (s *ImmuServer) ListUsers(ctx context.Context, req *empty.Empty) (*schema.ItemList, error) {
	items, err := s.getUsersLatestVersions(nil)
	if err != nil {
		return nil, err
	}
	itemList := &schema.ItemList{Items: make([]*schema.Item, 0, len(items))}
	for _, item := range items {
		item.Key = sysstore.TrimUserPrefix(item.Key)
		itemList.Items = append(itemList.Items, item)
	}
	return itemList, nil
}

func (s *ImmuServer) CreateUser(ctx context.Context, r *schema.CreateUserRequest) (*schema.CreateUserResponse, error) {
	if !auth.IsValidUsername(string(r.GetUser())) {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"username can only contain letters, digits and underscores")
	}
	items, err := s.getUsersLatestVersions(r.GetUser())
	if err != nil {
		s.Logger.Errorf("error checking if user already exists: %v", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	for _, item := range items {
		if !isFlaggedAsDeleted(item) {
			return nil, status.Errorf(codes.AlreadyExists, "username already exists")
		}
	}
	if err = auth.IsStrongPassword(string(r.GetPassword())); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	hashedPassword, err := auth.HashAndSaltPassword(string(r.GetPassword()))
	if err != nil {
		return nil, err
	}
	kv := schema.KeyValue{
		Key: auth.AddPermissionSuffix(
			sysstore.AddUserPrefix(r.GetUser()), r.GetPermissions()[0]),
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

func (s *ImmuServer) SetPermission(ctx context.Context, r *schema.Item) (*empty.Empty, error) {
	items, err := s.getUsersLatestVersions(r.GetKey())
	if err != nil {
		s.Logger.Errorf("error getting user: %v", err)
		return new(empty.Empty), status.Errorf(codes.Internal, "internal error")
	}
	var item *schema.Item
	for _, currItem := range items {
		if !isFlaggedAsDeleted(item) {
			item = currItem
			break
		}
	}
	if item == nil {
		return new(empty.Empty), status.Error(codes.NotFound, "user not fund")
	}
	kv := schema.KeyValue{
		Key:   auth.ReplacePermissionSuffix(item.GetKey(), r.GetValue()[0]),
		Value: item.GetValue(),
	}
	if _, err := s.SysStore.Set(kv); err != nil {
		s.Logger.Errorf("error persisting user with updated permission: %v", err)
		return new(empty.Empty), err
	}
	return new(empty.Empty), nil
}

func (s *ImmuServer) ChangePassword(ctx context.Context, r *schema.ChangePasswordRequest) (*empty.Empty, error) {
	e := new(empty.Empty)
	items, err := s.getUsersLatestVersions(r.GetUser())
	if err != nil {
		s.Logger.Errorf("error getting user: %v", err)
		return e, err
	}
	var item *schema.Item
	for _, currItem := range items {
		if !isFlaggedAsDeleted(item) {
			item = currItem
			break
		}
	}
	if item == nil {
		return e, status.Errorf(codes.NotFound, "user not found")
	}
	if string(r.GetUser()) == auth.AdminUsername {
		if err = auth.ComparePasswords(item.GetValue(), r.GetOldPassword()); err != nil {
			return e, status.Errorf(codes.PermissionDenied, "old password is incorrect")
		}
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
		Key:   item.GetKey(),
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
	items, err := s.getUsersLatestVersions(r.GetUser())
	if err != nil {
		s.Logger.Errorf("error getting user: %v", err)
		return e, err
	}
	var item *schema.Item
	for _, currItem := range items {
		if !isFlaggedAsDeleted(currItem) {
			item = currItem
			break
		}
	}
	if item == nil {
		return e, status.Errorf(codes.NotFound, "user not found")
	}
	kv := schema.KeyValue{
		Key:   item.GetKey(),
		Value: deletedFlag,
	}
	if _, err := s.SysStore.Set(kv); err != nil {
		s.Logger.Errorf("error deleting user: %v", err)
		return e, err
	}
	return e, nil
}
