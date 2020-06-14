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
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/codenotary/immudb/pkg/store/sysstore"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Db struct {
	Store    *store.Store
	SysStore *store.Store
	Logger   logger.Logger
	options  *DbOptions
}

// OpenDb Opens an existing Database from disk
func OpenDb(op *DbOptions) (*Db, error) {
	var err error
	db := &Db{
		Logger:  logger.NewSimpleLogger(op.GetDbName(), os.Stderr),
		options: op,
	}
	sysDbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName(), op.GetSysDbDir())
	dbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName(), op.GetDbDir())
	_, sysDbErr := os.Stat(sysDbDir)
	_, dbErr := os.Stat(dbDir)

	if os.IsNotExist(sysDbErr) || os.IsNotExist(dbErr) {
		return nil, fmt.Errorf("Missing database directories")
	}
	db.SysStore, err = store.Open(store.DefaultOptions(sysDbDir, db.Logger))
	if err != nil {
		db.Logger.Errorf("Unable to open sysstore: %s", err)
		return nil, err
	}
	db.Store, err = store.Open(store.DefaultOptions(dbDir, db.Logger))
	if err != nil {
		db.Logger.Errorf("Unable to open store: %s", err)
		return nil, err
	}
	return db, nil
}

// NewDb Creates a new Database along with it's directories and files
func NewDb(op *DbOptions) (*Db, error) {
	var err error
	db := &Db{
		Logger:  logger.NewSimpleLogger(op.GetDbName()+" ", os.Stderr),
		options: op,
	}
	sysDbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName(), op.GetSysDbDir())
	dbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName(), op.GetDbDir())
	_, sysDbErr := os.Stat(sysDbDir)
	_, dbErr := os.Stat(dbDir)
	if os.IsExist(sysDbErr) || os.IsExist(dbErr) {
		return nil, fmt.Errorf("Database directories already exist")
	}
	if err = os.MkdirAll(sysDbDir, os.ModePerm); err != nil {
		db.Logger.Errorf("Unable to create sys data folder: %s", err)
		return nil, err
	}

	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		db.Logger.Errorf("Unable to create data folder: %s", err)
		return nil, err
	}
	db.SysStore, err = store.Open(store.DefaultOptions(sysDbDir, db.Logger))
	if err != nil {
		db.Logger.Errorf("Unable to open sysstore: %s", err)
		return nil, err
	}
	db.Store, err = store.Open(store.DefaultOptions(dbDir, db.Logger))
	if err != nil {
		db.Logger.Errorf("Unable to open store: %s", err)
		return nil, err
	}
	return db, nil
}

func (d *Db) Set(ctx context.Context, kv *schema.KeyValue) (*schema.Index, error) {
	d.Logger.Debugf("set %s %d bytes", kv.Key, len(kv.Value))
	item, err := d.Store.Set(*kv)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (d *Db) Get(ctx context.Context, k *schema.Key) (*schema.Item, error) {
	item, err := d.Store.Get(*k)
	if item == nil {
		d.Logger.Debugf("get %s: item not found", k.Key)
	} else {
		d.Logger.Debugf("get %s %d bytes", k.Key, len(item.Value))
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}
func (d *Db) CurrentRoot(ctx context.Context, e *empty.Empty) (*schema.Root, error) {
	root, err := d.Store.CurrentRoot()
	if root != nil {
		d.Logger.Debugf("current root: %d %x", root.Index, root.Root)
	}
	return root, err
}

func (d *Db) SetSV(ctx context.Context, skv *schema.StructuredKeyValue) (*schema.Index, error) {
	kv, err := skv.ToKV()
	if err != nil {
		return nil, err
	}
	return d.Set(ctx, kv)
}
func (d *Db) GetSV(ctx context.Context, k *schema.Key) (*schema.StructuredItem, error) {
	it, err := d.Get(ctx, k)
	si, err := it.ToSItem()
	if err != nil {
		return nil, err
	}
	return si, err
}

func (d *Db) SafeSet(ctx context.Context, opts *schema.SafeSetOptions) (*schema.Proof, error) {
	d.Logger.Debugf("safeset %s %d bytes", opts.Kv.Key, len(opts.Kv.Value))
	item, err := d.Store.SafeSet(*opts)
	if err != nil {
		return nil, err
	}
	return item, nil
}
func (d *Db) SafeGet(ctx context.Context, opts *schema.SafeGetOptions) (*schema.SafeItem, error) {
	d.Logger.Debugf("safeget %s", opts.Key)
	sitem, err := d.Store.SafeGet(*opts)
	if err != nil {
		return nil, err
	}
	return sitem, nil
}
func (d *Db) SafeSetSV(ctx context.Context, sopts *schema.SafeSetSVOptions) (*schema.Proof, error) {
	kv, err := sopts.Skv.ToKV()
	if err != nil {
		return nil, err
	}
	opts := &schema.SafeSetOptions{
		Kv:        kv,
		RootIndex: sopts.RootIndex,
	}
	return d.SafeSet(ctx, opts)
}
func (d *Db) SafeGetSV(ctx context.Context, opts *schema.SafeGetOptions) (*schema.SafeStructuredItem, error) {
	it, err := d.SafeGet(ctx, opts)
	ssitem, err := it.ToSafeSItem()
	if err != nil {
		return nil, err
	}
	return ssitem, err
}

func (d *Db) SetBatch(ctx context.Context, kvl *schema.KVList) (*schema.Index, error) {
	d.Logger.Debugf("set batch %d", len(kvl.KVs))
	index, err := d.Store.SetBatch(*kvl)
	if err != nil {
		return nil, err
	}
	return index, nil
}

func (d *Db) GetBatch(ctx context.Context, kl *schema.KeyList) (*schema.ItemList, error) {
	list := &schema.ItemList{}
	for _, key := range kl.Keys {
		item, err := d.Store.Get(*key)
		if err == nil || err == store.ErrKeyNotFound {
			if item != nil {
				list.Items = append(list.Items, item)
			}
		} else {
			return nil, err
		}
	}
	return list, nil
}

func (d *Db) SetBatchSV(ctx context.Context, skvl *schema.SKVList) (*schema.Index, error) {
	kvl, err := skvl.ToKVList()
	if err != nil {
		return nil, err
	}
	return d.SetBatch(ctx, kvl)
}
func (d *Db) GetBatchSV(ctx context.Context, kl *schema.KeyList) (*schema.StructuredItemList, error) {
	list, err := d.GetBatch(ctx, kl)
	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (d *Db) ScanSV(ctx context.Context, opts *schema.ScanOptions) (*schema.StructuredItemList, error) {
	d.Logger.Debugf("scan %+v", *opts)
	list, err := d.Store.Scan(*opts)
	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (d *Db) Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.ItemsCount, error) {
	d.Logger.Debugf("count %s", prefix.Prefix)
	return d.Store.Count(*prefix)
}

func (d *Db) Inclusion(ctx context.Context, index *schema.Index) (*schema.InclusionProof, error) {
	d.Logger.Debugf("inclusion for index %d ", index.Index)
	proof, err := d.Store.InclusionProof(*index)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

func (d *Db) Consistency(ctx context.Context, index *schema.Index) (*schema.ConsistencyProof, error) {
	d.Logger.Debugf("consistency for index %d ", index.Index)
	proof, err := d.Store.ConsistencyProof(*index)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

func (d *Db) ByIndex(ctx context.Context, index *schema.Index) (*schema.Item, error) {
	d.Logger.Debugf("get by index %d ", index.Index)
	item, err := d.Store.ByIndex(*index)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (d *Db) ByIndexSV(ctx context.Context, index *schema.Index) (*schema.StructuredItem, error) {
	d.Logger.Debugf("get by index %d ", index.Index)
	item, err := d.Store.ByIndex(*index)
	if err != nil {
		return nil, err
	}
	sitem, err := item.ToSItem()
	if err != nil {
		return nil, err
	}
	return sitem, nil
}

func (d *Db) BySafeIndex(ctx context.Context, sio *schema.SafeIndexOptions) (*schema.SafeItem, error) {
	d.Logger.Debugf("get by safeIndex %d ", sio.Index)
	item, err := d.Store.BySafeIndex(*sio)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (d *Db) History(ctx context.Context, key *schema.Key) (*schema.ItemList, error) {
	d.Logger.Debugf("history for key %s ", string(key.Key))
	list, err := d.Store.History(*key)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (d *Db) HistorySV(ctx context.Context, key *schema.Key) (*schema.StructuredItemList, error) {
	d.Logger.Debugf("history for key %s ", string(key.Key))

	list, err := d.Store.History(*key)
	if err != nil {
		return nil, err
	}

	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (d *Db) Health(context.Context, *empty.Empty) (*schema.HealthResponse, error) {
	health := d.Store.HealthCheck()
	d.Logger.Debugf("health check: %v", health)
	return &schema.HealthResponse{Status: health}, nil
}

func (d *Db) Reference(ctx context.Context, refOpts *schema.ReferenceOptions) (index *schema.Index, err error) {
	index, err = d.Store.Reference(refOpts)
	if err != nil {
		return nil, err
	}
	d.Logger.Debugf("reference options: %v", refOpts)
	return index, nil
}

func (d *Db) SafeReference(ctx context.Context, safeRefOpts *schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	proof, err = d.Store.SafeReference(*safeRefOpts)
	if err != nil {
		return nil, err
	}
	d.Logger.Debugf("safe reference options: %v", safeRefOpts)
	return proof, nil
}

func (d *Db) ZAdd(ctx context.Context, opts *schema.ZAddOptions) (*schema.Index, error) {
	d.Logger.Debugf("zadd %+v", *opts)
	return d.Store.ZAdd(*opts)
}

func (d *Db) ZScan(ctx context.Context, opts *schema.ZScanOptions) (*schema.ItemList, error) {
	d.Logger.Debugf("zscan %+v", *opts)
	return d.Store.ZScan(*opts)
}

func (d *Db) ZScanSV(ctx context.Context, opts *schema.ZScanOptions) (*schema.StructuredItemList, error) {
	d.Logger.Debugf("zscan %+v", *opts)
	list, err := d.Store.ZScan(*opts)
	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (d *Db) SafeZAdd(ctx context.Context, opts *schema.SafeZAddOptions) (*schema.Proof, error) {
	d.Logger.Debugf("zadd %+v", *opts)
	return d.Store.SafeZAdd(*opts)
}

func (d *Db) IScan(ctx context.Context, opts *schema.IScanOptions) (*schema.Page, error) {
	d.Logger.Debugf("iscan %+v", *opts)
	return d.Store.IScan(*opts)
}

func (d *Db) IScanSV(ctx context.Context, opts *schema.IScanOptions) (*schema.SPage, error) {
	d.Logger.Debugf("zscan %+v", *opts)
	page, err := d.Store.IScan(*opts)
	SPage, err := page.ToSPage()
	if err != nil {
		return nil, err
	}
	return SPage, err
}

func (d *Db) Dump(in *empty.Empty, stream schema.ImmuService_DumpServer) error {
	kvChan := make(chan *pb.KVList)
	done := make(chan bool)

	retrieveLists := func() {
		for {
			list, more := <-kvChan
			if more {
				stream.Send(list)
			} else {
				done <- true
				return
			}
		}
	}

	go retrieveLists()
	err := d.Store.Dump(kvChan)
	<-done

	d.Logger.Debugf("Dump stream complete")
	return err
}
func (d *Db) isUserDeactivated(user *schema.Item) error {
	permission, err := d.getUserPermissions(user.GetIndex())
	if err != nil {
		return err
	}
	if permission == auth.PermissionNone {
		return ErrUserDeactivated
	}
	return nil
}
func (d *Db) getUser(username []byte, includeDeactivated bool) (*schema.Item, error) {
	key := make([]byte, 1+len(username))
	key[0] = sysstore.KeyPrefixUser
	copy(key[1:], username)
	item, err := d.SysStore.Get(schema.Key{Key: key})
	if err != nil {
		return nil, err
	}
	if !includeDeactivated {
		if err := d.isUserDeactivated(item); err != nil {
			return nil, err
		}
	}
	item.Key = item.GetKey()[1:]
	return item, nil
}
func (d *Db) getUserAttr(userIndex uint64, attrPrefix byte) ([]byte, error) {
	key := make([]byte, 1+8)
	key[0] = attrPrefix
	binary.BigEndian.PutUint64(key[1:], userIndex)
	item, err := d.SysStore.Get(schema.Key{Key: key})
	if err != nil {
		return nil, err
	}
	return item.GetValue(), nil
}
func (d *Db) getUserPassword(userIndex uint64) ([]byte, error) {
	return d.getUserAttr(userIndex, sysstore.KeyPrefixPassword)
}
func (d *Db) getUserPermissions(userIndex uint64) (byte, error) {
	ps, err := d.getUserAttr(userIndex, sysstore.KeyPrefixPermissions)
	if err != nil {
		return 0, err
	}
	return ps[0], nil
}

// DeactivateUser ...
func (d *Db) DeactivateUser(ctx context.Context, r *schema.UserRequest) (*empty.Empty, error) {
	item, err := d.getUser(r.GetUser(), false)
	if err != nil {
		return new(empty.Empty), err
	}
	if item == nil {
		return new(empty.Empty),
			status.Errorf(codes.NotFound, "user not found or is already deactivated")
	}
	permissions, err := d.getUserPermissions(item.GetIndex())
	if err != nil {
		return nil, err
	}
	if permissions == auth.PermissionAdmin {
		return nil, status.Errorf(
			codes.PermissionDenied, "deactivating admin user is not allowed")
	}
	permissionsKey := make([]byte, 1+8)
	permissionsKey[0] = sysstore.KeyPrefixPermissions
	binary.BigEndian.PutUint64(permissionsKey[1:], item.GetIndex())
	permissionsKV :=
		schema.KeyValue{Key: permissionsKey, Value: []byte{auth.PermissionNone}}
	if _, err := d.SysStore.Set(permissionsKV); err != nil {
		d.Logger.Errorf("error saving user permissions to deactivate user: %v", err)
		return new(empty.Empty), err
	}
	auth.DropTokenKeys(string(r.GetUser()))
	return new(empty.Empty), nil
}

// ErrUserDeactivated ...
var ErrUserDeactivated = errors.New("user is deactivated")

func (d *Db) getUsers(includeDeactivated bool) (*schema.ItemList, error) {
	itemList, err := d.SysStore.Scan(schema.ScanOptions{
		Prefix: []byte{sysstore.KeyPrefixUser},
	})
	if err != nil {
		d.Logger.Errorf("error getting users: %v", err)
		return nil, err
	}
	for i := 0; i < len(itemList.Items); i++ {
		if !includeDeactivated {
			if err := d.isUserDeactivated(itemList.Items[i]); err != nil {
				continue
			}
		}
		itemList.Items[i].Key = itemList.Items[i].Key[1:]
	}
	return itemList, nil
}

func (d *Db) saveUser(
	username []byte, hashedPassword []byte, permissions byte) error {
	// TODO OGG: check with Michele how to wrap all Sets in a transaction
	// Set user
	userKey := make([]byte, 1+len(username))
	userKey[0] = sysstore.KeyPrefixUser
	copy(userKey[1:], username)
	userKV := schema.KeyValue{Key: userKey, Value: username}
	userIndex, err := d.SysStore.Set(userKV)
	if err != nil {
		d.Logger.Errorf("error saving user: %v", err)
		return err
	}
	// Set password
	passKey := make([]byte, 1+8)
	passKey[0] = sysstore.KeyPrefixPassword
	binary.BigEndian.PutUint64(passKey[1:], userIndex.GetIndex())
	passKV := schema.KeyValue{Key: passKey, Value: hashedPassword}
	if _, err := d.SysStore.Set(passKV); err != nil {
		d.Logger.Errorf("error saving user password: %v", err)
		return err
	}
	// Set permissions
	permissionsKey := make([]byte, 1+8)
	permissionsKey[0] = sysstore.KeyPrefixPermissions
	binary.BigEndian.PutUint64(permissionsKey[1:], userIndex.GetIndex())
	permissionsKV :=
		schema.KeyValue{Key: permissionsKey, Value: []byte{permissions}}
	if _, err := d.SysStore.Set(permissionsKV); err != nil {
		d.Logger.Errorf("error saving user permissions: %v", err)
		return err
	}
	return nil
}

// getUserData returns only active userdata (username,hashed password, permision) from username
func (d *Db) getUserData(username []byte) (*auth.User, error) {
	var permissions byte
	item, err := d.getUser(username, false)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return nil, status.Errorf(codes.PermissionDenied, "invalid user or password")
		}
		return nil, err
	}
	permissions, err = d.getUserPermissions(item.GetIndex())
	if err != nil {
		return nil, err
	}
	hashedPassword, err := d.getUserPassword(item.GetIndex())
	if err != nil {
		return nil, err
	}
	return &auth.User{
		Username:       string(item.GetKey()),
		HashedPassword: hashedPassword,
		Permissions:    permissions,
	}, nil
}

// userExists checks if user with username exists
// if permision != PermissionNone then it also checks the permission matches
// if password is not empty then it also checks the password
// Returns user object and isActive if all requested condintions match
// Returns error if the requested conditions do not match
func (d *Db) userExists(username []byte, permission byte, password []byte) (*auth.User, error) {
	userdata, err := d.getUserData(username)
	if err != nil {
		return nil, err
	}
	if (permission != auth.PermissionNone) && (userdata.Permissions != permission) {
		return nil, fmt.Errorf("User with this permision does not exist")
	}
	err = userdata.ComparePasswords(password)
	if (len(password) != 0) && (err != nil) {
		return nil, status.Errorf(codes.PermissionDenied, "invalid user or password")
	}
	return userdata, nil
}

// ListUsers ...
func (d *Db) ListUsers(ctx context.Context, req *empty.Empty) (*schema.UserList, error) {
	itemList, err := d.getUsers(true)
	if err != nil {
		return nil, err
	}
	users := make([]*schema.User, len(itemList.Items))
	for i, item := range itemList.Items {
		permissions, err := d.getUserPermissions(item.GetIndex())
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

// CreateAdminUser assings admin user to database.
// A new password is generated automatically.
// returns username, plain password, error
func (d *Db) CreateAdminUser(username []byte) ([]byte, []byte, error) {
	username, plainPass, err := d.CreateUser(username, []byte{}, auth.PermissionAdmin, false)
	if err == nil {
		return nil, nil, fmt.Errorf(
			"user exists or there was an error determining if admin user exists: %v", err)
	}
	return username, plainPass, nil
}

// CreateUser creates a user and returns username and plain text password
// A new password is generated automatically if passed parameter is empty
// If enforceStrongAuth is true it checks if username and password meet security criteria
func (d *Db) CreateUser(username []byte, password []byte, permission byte, enforceStrongAuth bool) ([]byte, []byte, error) {
	if enforceStrongAuth {
		if !auth.IsValidUsername(string(username)) {
			return nil, nil, status.Errorf(
				codes.InvalidArgument,
				"username can only contain letters, digits and underscores")
		}
	}
	userdata, err := d.userExists(username, auth.PermissionNone, nil)
	if err == nil {
		err = fmt.Errorf(
			"user exists or there was an error determining if admin user exists: %v", err)
		d.Logger.Errorf("error checking if user already exists: %v", err)
		return nil, nil, err
	}
	if enforceStrongAuth {
		if err := auth.IsStrongPassword(string(password)); err != nil {
			return nil, nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
	}
	userdata = new(auth.User)
	plainpassword, err := userdata.GenerateOrSetPassword(password)
	if err != nil {
		return nil, nil, err
	}
	//TODO gj please check that the passed permission is in our list.
	//Someone could cause a mess with that
	if err := d.saveUser(username, userdata.HashedPassword, permission); err != nil {
		return nil, nil, err
	}
	return username, plainpassword, nil
}

// SetPermission ...
func (d *Db) SetPermission(ctx context.Context, r *schema.Item) (*empty.Empty, error) {
	if len(r.GetValue()) <= 0 {
		return new(empty.Empty), status.Errorf(
			codes.InvalidArgument, "no permission specified")
	}
	if int(r.GetValue()[0]) == auth.PermissionAdmin {
		return new(empty.Empty), status.Error(
			codes.PermissionDenied, "admin permission is not allowed to be set")
	}
	item, err := d.getUser(r.GetKey(), true)
	if err != nil {
		return new(empty.Empty), err
	}
	if item == nil {
		return new(empty.Empty), status.Error(codes.NotFound, "user not found")
	}
	permissionsKey := make([]byte, 1+8)
	permissionsKey[0] = sysstore.KeyPrefixPermissions
	binary.BigEndian.PutUint64(permissionsKey[1:], item.GetIndex())
	permissionsKV :=
		schema.KeyValue{Key: permissionsKey, Value: r.GetValue()}
	if _, err := d.SysStore.Set(permissionsKV); err != nil {
		d.Logger.Errorf("error saving user permissions: %v", err)
		return new(empty.Empty), err
	}
	auth.DropTokenKeys(string(r.GetKey()))
	return new(empty.Empty), nil
}

// ChangePassword ...
func (d *Db) ChangePassword(ctx context.Context, r *schema.ChangePasswordRequest) (*empty.Empty, error) {
	item, err := d.getUser(r.GetUser(), false)
	if err != nil {
		return new(empty.Empty), err
	}
	if item == nil {
		return new(empty.Empty), status.Errorf(codes.NotFound, "user not found")
	}
	oldHashedPassword, err := d.getUserPassword(item.GetIndex())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting user password: %v", err)
	}
	if string(r.GetUser()) == auth.AdminUsername {
		if err = auth.ComparePasswords(oldHashedPassword, r.GetOldPassword()); err != nil {
			return new(empty.Empty), status.Errorf(codes.PermissionDenied, "old password is incorrect")
		}
	}
	newPass := r.GetNewPassword()
	if err = auth.IsStrongPassword(string(newPass)); err != nil {
		return new(empty.Empty), status.Errorf(codes.InvalidArgument, "%v", err)
	}
	hashedPassword, err := auth.HashAndSaltPassword(newPass)
	if err != nil {
		return new(empty.Empty), status.Errorf(codes.Internal, "%v", err)
	}
	passKey := make([]byte, 1+8)
	passKey[0] = sysstore.KeyPrefixPassword
	binary.BigEndian.PutUint64(passKey[1:], item.GetIndex())
	passKV := schema.KeyValue{Key: passKey, Value: hashedPassword}
	if _, err := d.SysStore.Set(passKV); err != nil {
		d.Logger.Errorf("error saving user password: %v", err)
		return new(empty.Empty), err
	}
	return new(empty.Empty), nil
}

// PrintTree ...
func (d *Db) PrintTree(context.Context, *empty.Empty) (*schema.Tree, error) {
	tree := d.Store.GetTree()
	return tree, nil
}

// Login Authenticate user
func (d *Db) Login(ctx context.Context, username []byte, password []byte) (*auth.User, error) {
	user, err := d.userExists(username, auth.PermissionNone, password)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "invalid user or password")
	}
	return user, nil
}

// GenerateDbID generate and ID for the database
func GenerateDbID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}
