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
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/codenotary/immudb/pkg/store/sysstore"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrUserDeactivated = errors.New("user is deactivated")

func (s *ImmuServer) isUserDeactivated(user *schema.Item) error {
	permission, err := s.getUserPermissions(user.GetIndex())
	if err != nil {
		return err
	}
	if permission == auth.PermissionNone {
		return ErrUserDeactivated
	}
	return nil
}

func (s *ImmuServer) getUser(username []byte, includeDeactivated bool) (*schema.Item, error) {
	key := make([]byte, 1+len(username))
	key[0] = sysstore.KeysPrefixes.User
	copy(key[1:], username)
	item, err := s.SysStore.Get(schema.Key{Key: key})
	if err != nil {
		return nil, err
	}
	if !includeDeactivated {
		if err := s.isUserDeactivated(item); err != nil {
			return nil, err
		}
	}
	item.Key = item.GetKey()[1:]
	return item, nil
}
func (s *ImmuServer) getUserAttr(userIndex uint64, attrPrefix byte) ([]byte, error) {
	key := make([]byte, 1+8)
	key[0] = attrPrefix
	binary.BigEndian.PutUint64(key[1:], userIndex)
	item, err := s.SysStore.Get(schema.Key{Key: key})
	if err != nil {
		return nil, err
	}
	return item.GetValue(), nil
}
func (s *ImmuServer) getUserPassword(userIndex uint64) ([]byte, error) {
	return s.getUserAttr(userIndex, sysstore.KeysPrefixes.Password)
}
func (s *ImmuServer) getUserPermissions(userIndex uint64) (byte, error) {
	ps, err := s.getUserAttr(userIndex, sysstore.KeysPrefixes.Permissions)
	if err != nil {
		return 0, err
	}
	return ps[0], nil
}

func (s *ImmuServer) getUsers(includeDeactivated bool) (*schema.ItemList, error) {
	itemList, err := s.SysStore.Scan(schema.ScanOptions{
		Prefix: []byte{sysstore.KeysPrefixes.User},
	})
	if err != nil {
		s.Logger.Errorf("error getting users: %v", err)
		return nil, err
	}
	for i := 0; i < len(itemList.Items); i++ {
		if !includeDeactivated {
			if err := s.isUserDeactivated(itemList.Items[i]); err != nil {
				continue
			}
		}
		itemList.Items[i].Key = itemList.Items[i].Key[1:]
	}
	return itemList, nil
}

func (s *ImmuServer) saveUser(
	username []byte, hashedPassword []byte, permissions byte) error {
	// TODO OGG: check with Michele how to wrap all Sets in a transaction
	// Set user
	userKey := make([]byte, 1+len(username))
	userKey[0] = sysstore.KeysPrefixes.User
	copy(userKey[1:], username)
	userKV := schema.KeyValue{Key: userKey, Value: username}
	userIndex, err := s.SysStore.Set(userKV)
	if err != nil {
		s.Logger.Errorf("error saving user: %v", err)
		return err
	}
	// Set password
	passKey := make([]byte, 1+8)
	passKey[0] = sysstore.KeysPrefixes.Password
	binary.BigEndian.PutUint64(passKey[1:], userIndex.GetIndex())
	passKV := schema.KeyValue{Key: passKey, Value: hashedPassword}
	if _, err := s.SysStore.Set(passKV); err != nil {
		s.Logger.Errorf("error saving user password: %v", err)
		return err
	}
	// Set permissions
	permissionsKey := make([]byte, 1+8)
	permissionsKey[0] = sysstore.KeysPrefixes.Permissions
	binary.BigEndian.PutUint64(permissionsKey[1:], userIndex.GetIndex())
	permissionsKV :=
		schema.KeyValue{Key: permissionsKey, Value: []byte{permissions}}
	if _, err := s.SysStore.Set(permissionsKV); err != nil {
		s.Logger.Errorf("error saving user permissions: %v", err)
		return err
	}
	return nil
}

func (s *ImmuServer) adminUserExists(ctx context.Context) (bool, error) {
	return s.isAdminUser(ctx, []byte(auth.AdminUsername))
}
func (s *ImmuServer) isAdminUser(ctx context.Context, username []byte) (bool, error) {
	item, err := s.getUser(username, false)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	permissions, err := s.getUserPermissions(item.GetIndex())
	if err != nil {
		return false, err
	}
	return permissions == auth.PermissionAdmin, nil
}

func (s *ImmuServer) CreateAdminUser(ctx context.Context) (string, string, error) {
	exists, err := s.adminUserExists(ctx)
	if err != nil {
		return "", "", fmt.Errorf(
			"error determining if admin user exists: %v", err)
	}
	if exists {
		return "", "", status.Error(codes.AlreadyExists, "admin user already exists")
	}
	u := auth.User{Username: auth.AdminUsername}
	plainPass, err := u.GenerateAndSetPassword()
	if err != nil {
		s.Logger.Errorf("error generating password for admin user: %v", err)
	}
	if err = s.saveUser([]byte(u.Username), u.HashedPassword, auth.PermissionAdmin); err != nil {
		return "", "", err
	}
	return u.Username, plainPass, nil
}

func (s *ImmuServer) ListUsers(ctx context.Context, req *empty.Empty) (*schema.UserList, error) {
	itemList, err := s.getUsers(true)
	if err != nil {
		return nil, err
	}
	users := make([]*schema.User, len(itemList.Items))
	for i, item := range itemList.Items {
		permissions, err := s.getUserPermissions(item.GetIndex())
		if err != nil {
			return nil, err
		}
		users[i] = &schema.User{
			User:        item.GetKey(),
			Permissions: []byte{permissions},
		}
	}
	return &schema.UserList{Users: users}, nil
}

func (s *ImmuServer) GetUser(ctx context.Context, r *schema.UserRequest) (*schema.UserResponse, error) {
	user, err := s.getUser(r.GetUser(), true)
	if err != nil {
		return nil, err
	}
	permissions, err := s.getUserPermissions(user.GetIndex())
	if err != nil {
		return nil, err
	}
	return &schema.UserResponse{User: user.GetKey(), Permissions: []byte{permissions}}, nil
}

func (s *ImmuServer) CreateUser(ctx context.Context, r *schema.CreateUserRequest) (*schema.UserResponse, error) {
	if !auth.IsValidUsername(string(r.GetUser())) {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"username can only contain letters, digits and underscores")
	}
	if _, err := s.getUser(r.GetUser(), true); err != store.ErrKeyNotFound {
		if err == nil {
			return nil, status.Errorf(codes.AlreadyExists, "user already exists")
		}
		s.Logger.Errorf("error checking if user already exists: %v", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	if err := auth.IsStrongPassword(string(r.GetPassword())); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	hashedPassword, err := auth.HashAndSaltPassword(string(r.GetPassword()))
	if err != nil {
		return nil, err
	}
	if err := s.saveUser(r.GetUser(), hashedPassword, r.GetPermissions()[0]); err != nil {
		return nil, err
	}
	return &schema.UserResponse{User: r.GetUser(), Permissions: r.GetPermissions()}, nil
}

func (s *ImmuServer) SetPermission(ctx context.Context, r *schema.Item) (*empty.Empty, error) {
	item, err := s.getUser(r.GetKey(), true)
	if err != nil {
		return new(empty.Empty), err
	}
	if item == nil {
		return new(empty.Empty), status.Error(codes.NotFound, "user not found")
	}
	permissionsKey := make([]byte, 1+8)
	permissionsKey[0] = sysstore.KeysPrefixes.Permissions
	binary.BigEndian.PutUint64(permissionsKey[1:], item.GetIndex())
	permissionsKV :=
		schema.KeyValue{Key: permissionsKey, Value: r.GetValue()}
	if _, err := s.SysStore.Set(permissionsKV); err != nil {
		s.Logger.Errorf("error saving user permissions: %v", err)
		return new(empty.Empty), err
	}
	return new(empty.Empty), nil
}

func (s *ImmuServer) ChangePassword(ctx context.Context, r *schema.ChangePasswordRequest) (*empty.Empty, error) {
	item, err := s.getUser(r.GetUser(), false)
	if err != nil {
		return new(empty.Empty), err
	}
	if item == nil {
		return new(empty.Empty), status.Errorf(codes.NotFound, "user not found")
	}
	oldHashedPassword, err := s.getUserPassword(item.GetIndex())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting user password: %v", err)
	}
	if string(r.GetUser()) == auth.AdminUsername {
		if err = auth.ComparePasswords(oldHashedPassword, r.GetOldPassword()); err != nil {
			return new(empty.Empty), status.Errorf(codes.PermissionDenied, "old password is incorrect")
		}
	}
	newPass := string(r.GetNewPassword())
	if err = auth.IsStrongPassword(newPass); err != nil {
		return new(empty.Empty), status.Errorf(codes.InvalidArgument, "%v", err)
	}
	hashedPassword, err := auth.HashAndSaltPassword(newPass)
	if err != nil {
		return new(empty.Empty), status.Errorf(codes.Internal, "%v", err)
	}
	passKey := make([]byte, 1+8)
	passKey[0] = sysstore.KeysPrefixes.Password
	binary.BigEndian.PutUint64(passKey[1:], item.GetIndex())
	passKV := schema.KeyValue{Key: passKey, Value: hashedPassword}
	if _, err := s.SysStore.Set(passKV); err != nil {
		s.Logger.Errorf("error saving user password: %v", err)
		return new(empty.Empty), err
	}
	return new(empty.Empty), nil
}

func (s *ImmuServer) DeactivateUser(ctx context.Context, r *schema.UserRequest) (*empty.Empty, error) {
	item, err := s.getUser(r.GetUser(), false)
	if err != nil {
		return new(empty.Empty), err
	}
	if item == nil {
		return new(empty.Empty),
			status.Errorf(codes.NotFound, "user not found or is already deactivated")
	}
	permissionsKey := make([]byte, 1+8)
	permissionsKey[0] = sysstore.KeysPrefixes.Permissions
	binary.BigEndian.PutUint64(permissionsKey[1:], item.GetIndex())
	permissionsKV :=
		schema.KeyValue{Key: permissionsKey, Value: []byte{auth.PermissionNone}}
	if _, err := s.SysStore.Set(permissionsKV); err != nil {
		s.Logger.Errorf("error saving user permissions to deactivate user: %v", err)
		return new(empty.Empty), err
	}
	return new(empty.Empty), nil
}
