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
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

var testDatabase = "lisbon"
var testUsername = []byte("Sagrada")
var testPassword = []byte("Familia@2")
var testKey = []byte("Antoni")
var testValue = []byte("Gaudí")

func newInmemoryAuthServer() *ImmuServer {
	dbRootpath := DefaultOption().GetDbRootPath()
	s := DefaultServer()
	s = s.WithOptions(s.Options.WithAuth(true).WithInMemoryStore(true))
	err := s.loadDefaultDatabase(dbRootpath)
	if err != nil {
		log.Fatal(err)
	}
	err = s.loadSystemDatabase(dbRootpath)
	if err != nil {
		log.Fatal(err)
	}
	return s
}
func newAuthServer() *ImmuServer {
	dbRootpath := DefaultOption().GetDbRootPath()
	s := DefaultServer()
	s = s.WithOptions(s.Options.WithAuth(true))
	err := s.loadDefaultDatabase(dbRootpath)
	if err != nil {
		log.Fatal(err)
	}
	err = s.loadSystemDatabase(dbRootpath)
	if err != nil {
		log.Fatal(err)
	}
	return s
}
func loginSysAdmin(s *ImmuServer) (context.Context, error) {
	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	l, err := s.Login(ctx, r)
	if err != nil {
		log.Fatal(err)
	}
	m := make(map[string]string)
	m["Authorization"] = "Bearer " + string(l.Token)
	ctx = metadata.NewIncomingContext(ctx, metadata.New(m))
	return ctx, nil
}
func TestDefaultDatabaseLoad(t *testing.T) {
	options := DefaultOption()
	dbRootpath := options.GetDbRootPath()
	s := DefaultServer()
	err := s.loadDefaultDatabase(dbRootpath)
	if err != nil {
		t.Errorf("error loading default database %v", err)
	}
	defer func() {
		os.RemoveAll(dbRootpath)
	}()
	_, err = os.Stat(path.Join(options.GetDbRootPath(), DefaultOptions().defaultDbName))
	if os.IsNotExist(err) {
		t.Errorf("default database directory not created")
	}
}
func TestSystemDatabaseLoad(t *testing.T) {
	options := DefaultOption()
	dbRootpath := options.GetDbRootPath()
	s := DefaultServer()
	err := s.loadDefaultDatabase(dbRootpath)
	if err != nil {
		t.Errorf("error loading default database %v", err)
	}
	err = s.loadSystemDatabase(dbRootpath)
	if err != nil {
		t.Errorf("error loading system database %v", err)
	}
	defer func() {
		os.RemoveAll(dbRootpath)
	}()
	_, err = os.Stat(path.Join(options.GetDbRootPath(), DefaultOptions().GetSystemAdminDbName()))
	if os.IsNotExist(err) {
		t.Errorf("system database directory not created")
	}
}
func TestLogin(t *testing.T) {
	s := newInmemoryAuthServer()
	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	resp, err := s.Login(context.Background(), r)
	if err != nil {
		t.Errorf("Login error %v", err)
	}
	if len(resp.Token) == 0 {
		t.Errorf("login token is empty")
	}
	if len(resp.Warning) == 0 {
		t.Errorf("default immudb password missing warning")
	}
}
func TestLogout(t *testing.T) {
	s := newInmemoryAuthServer()
	_, err := s.Logout(context.Background(), &emptypb.Empty{})
	if err.Error() != "rpc error: code = Internal desc = no headers found on request" {
		t.Errorf("Logout expected error, got %v", err)
	}

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	l, err := s.Login(ctx, r)
	if err != nil {
		t.Errorf("Login error %v", err)
	}
	m := make(map[string]string)
	m["Authorization"] = "Bearer " + string(l.Token)
	ctx = metadata.NewIncomingContext(ctx, metadata.New(m))
	_, err = s.Logout(ctx, &emptypb.Empty{})
	if err != nil {
		t.Errorf("Logout error %v", err)
	}
}
func TestCreateDatabase(t *testing.T) {
	s := newInmemoryAuthServer()
	ctx, err := loginSysAdmin(s)
	if err != nil {
		t.Errorf("Login error %v", err)
	}
	newdb := &schema.Database{
		Databasename: "Lisbon",
	}
	dbrepl, err := s.CreateDatabase(ctx, newdb)
	if err != nil {
		t.Errorf("Createdatabase error %v", err)
	}
	if dbrepl.Error.Errorcode != schema.ErrorCodes_Ok {
		t.Errorf("Createdatabase error %v", dbrepl)
	}
}

func testCreateUser(ctx context.Context, s *ImmuServer, t *testing.T) {
	newUser := &schema.CreateUserRequest{
		User:       testUsername,
		Password:   testPassword,
		Database:   testDatabase,
		Permission: auth.PermissionAdmin,
	}
	userresp, err := s.CreateUser(ctx, newUser)
	if err != nil {
		t.Errorf("CreateUser error %v", err)
	}
	if !bytes.Equal(userresp.User, testUsername) {
		t.Errorf("CreateUser error username does not match %v", userresp)
	}
	if userresp.Permission != auth.PermissionAdmin {
		t.Errorf("CreateUser error permission does not match %v", userresp)
	}
}
func testListUsers(ctx context.Context, s *ImmuServer, t *testing.T) {
	users, err := s.ListUsers(ctx, &emptypb.Empty{})
	if err != nil {
		t.Errorf("ListUsers error %v", err)
	}
	if len(users.Users) < 1 {
		t.Errorf("List users, expected >1 got %v", len(users.Users))
	}
}
func testListDatabases(ctx context.Context, s *ImmuServer, t *testing.T) {
	dbs, err := s.DatabaseList(ctx, &emptypb.Empty{})
	if err != nil {
		t.Errorf("DatabaseList error %v", err)
	}
	if len(dbs.Databases) < 1 {
		t.Errorf("List databases, expected >1 got %v", len(dbs.Databases))
	}
}
func testUseDatabase(ctx context.Context, s *ImmuServer, t *testing.T) {
	dbs, err := s.UseDatabase(ctx, &schema.Database{
		Databasename: testDatabase,
	})
	if err != nil {
		t.Errorf("UseDatabase error %v", err)
	}
	if dbs.Error.Errorcode != schema.ErrorCodes_Ok {
		t.Errorf("Error selecting database %v", dbs)
	}
	if len(dbs.Token) == 0 {
		t.Errorf("Expected token, got %v", dbs.Token)
	}
	m := make(map[string]string)
	m["Authorization"] = "Bearer " + string(dbs.Token)
	ctx = metadata.NewIncomingContext(ctx, metadata.New(m))
}
func testChangePermission(ctx context.Context, s *ImmuServer, t *testing.T) {
	perm, err := s.ChangePermission(ctx, &schema.ChangePermissionRequest{
		Action:     schema.PermissionAction_GRANT,
		Database:   testDatabase,
		Permission: auth.PermissionR,
		Username:   string(testUsername),
	})
	if err != nil {
		t.Errorf("DatabaseList error %v", err)
	}
	if perm.Errorcode != schema.ErrorCodes_Ok {
		t.Errorf("error changing permission, got %v", perm)
	}
}
func testDeactivateUser(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.SetActiveUser(ctx, &schema.SetActiveUserRequest{
		Active:   false,
		Username: string(testUsername),
	})
	if err != nil {
		t.Errorf("DeactivateUser error %v", err)
	}
}
func testSetActiveUser(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.SetActiveUser(ctx, &schema.SetActiveUserRequest{
		Active:   true,
		Username: string(testUsername),
	})
	if err != nil {
		t.Errorf("SetActiveUser error %v", err)
	}
}
func testChangePassword(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.ChangePassword(ctx, &schema.ChangePasswordRequest{
		NewPassword: testPassword,
		OldPassword: testPassword,
		User:        testUsername,
	})
	if err != nil {
		t.Errorf("ChangePassword error %v", err)
	}
}

func testSetGet(ctx context.Context, s *ImmuServer, t *testing.T) {
	ind, err := s.Set(ctx, &schema.KeyValue{
		Key:   testKey,
		Value: testValue,
	})
	if err != nil {
		t.Errorf("set error %v", err)
	}

	it, err := s.Get(ctx, &schema.Key{
		Key: testKey,
	})
	if err != nil {
		t.Errorf("Get error %v", err)
	}
	if it.Index != ind.Index {
		t.Errorf("set.get index missmatch expected %v got %v", ind, it.Index)
	}
	if !bytes.Equal(it.Key, testKey) {
		t.Errorf("get key missmatch expected %v got %v", string(testKey), string(it.Key))
	}
	if !bytes.Equal(it.Value, testValue) {
		t.Errorf("get key missmatch expected %v got %v", string(testValue), string(it.Value))
	}
}
func testSafeSetGet(ctx context.Context, s *ImmuServer, t *testing.T) {
	root, err := s.CurrentRoot(ctx, &emptypb.Empty{})
	if err != nil {
		t.Error(err)
	}
	kv := []*schema.SafeSetOptions{
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Alberto"),
				Value: []byte("Tomba"),
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Jean-Claude"),
				Value: []byte("Killy"),
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Franz"),
				Value: []byte("Clamer"),
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		},
	}
	for _, val := range kv {
		proof, err := s.SafeSet(ctx, val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if proof == nil {
			t.Errorf("Nil proof after SafeSet")
		}
		it, err := s.SafeGet(ctx, &schema.SafeGetOptions{
			Key: val.Kv.Key,
		})
		if it.GetItem().GetIndex() != proof.Index {
			t.Errorf("SafeGet index error, expected %d, got %d", proof.Index, it.GetItem().GetIndex())
		}
	}
}

func testCurrentRoot(ctx context.Context, s *ImmuServer, t *testing.T) {
	for _, val := range kv {
		_, err := s.Set(ctx, val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		_, err = s.CurrentRoot(ctx, &emptypb.Empty{})
		if err != nil {
			t.Errorf("Error getting current root %s", err)
		}
	}
}
func testSVSetGet(ctx context.Context, s *ImmuServer, t *testing.T) {
	for _, val := range Skv.SKVs {
		it, err := s.SetSV(ctx, val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		k := &schema.Key{
			Key: []byte(val.Key),
		}
		item, err := s.GetSV(ctx, k)
		if err != nil {
			t.Errorf("Error reading key %s", err)
		}

		if it.GetIndex() != item.Index {
			t.Errorf("index error expecting %v got %v", item.Index, it.GetIndex())
		}
		if !bytes.Equal(item.GetKey(), val.Key) {
			t.Errorf("Inserted Key not equal to read Key")
		}
		sk := item.GetValue()
		if sk.GetTimestamp() != val.GetValue().GetTimestamp() {
			t.Errorf("Inserted value not equal to read value")
		}
		if !bytes.Equal(sk.GetPayload(), val.GetValue().Payload) {
			t.Errorf("Inserted Payload not equal to read value")
		}
	}
}
func testSafeSetGetSV(ctx context.Context, s *ImmuServer, t *testing.T) {
	root, err := s.CurrentRoot(ctx, &emptypb.Empty{})
	if err != nil {
		t.Error(err)
	}
	SafeSkv := []*schema.SafeSetSVOptions{
		{
			Skv: &schema.StructuredKeyValue{
				Key: []byte("Alberto"),
				Value: &schema.Content{
					Timestamp: uint64(time.Now().Unix()),
					Payload:   []byte("Tomba"),
				},
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		},
		{
			Skv: &schema.StructuredKeyValue{
				Key: []byte("Jean-Claude"),
				Value: &schema.Content{
					Timestamp: uint64(time.Now().Unix()),
					Payload:   []byte("Killy"),
				},
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		},
		{
			Skv: &schema.StructuredKeyValue{
				Key: []byte("Franz"),
				Value: &schema.Content{
					Timestamp: uint64(time.Now().Unix()),
					Payload:   []byte("Clamer"),
				},
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		},
	}
	for _, val := range SafeSkv {
		proof, err := s.SafeSetSV(ctx, val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if proof == nil {
			t.Errorf("Nil proof after SafeSet")
		}
		it, err := s.SafeGetSV(ctx, &schema.SafeGetOptions{
			Key: val.Skv.Key,
		})
		if it.GetItem().GetIndex() != proof.Index {
			t.Errorf("SafeGet index error, expected %d, got %d", proof.Index, it.GetItem().GetIndex())
		}
	}
}
func testSetGetBatch(ctx context.Context, s *ImmuServer, t *testing.T) {
	Skv := &schema.KVList{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte("Alberto"),
				Value: []byte("Tomba"),
			},
			{
				Key:   []byte("Jean-Claude"),
				Value: []byte("Killy"),
			},
			{
				Key:   []byte("Franz"),
				Value: []byte("Clamer"),
			},
		},
	}
	ind, err := s.SetBatch(ctx, Skv)
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if ind == nil {
		t.Errorf("Nil index after Setbatch")
	}

	itList, err := s.GetBatch(ctx, &schema.KeyList{
		Keys: []*schema.Key{
			{
				Key: []byte("Alberto"),
			},
			{
				Key: []byte("Jean-Claude"),
			},
			{
				Key: []byte("Franz"),
			},
		},
	})
	for ind, val := range itList.Items {
		if !bytes.Equal(val.Value, Skv.KVs[ind].Value) {
			t.Errorf("BatchSet value not equal to BatchGet value, expected %s, got %s", string(Skv.KVs[ind].Value), string(val.Value))
		}
	}
}
func testSetGetBatchSV(ctx context.Context, s *ImmuServer, t *testing.T) {
	ind, err := s.SetBatchSV(ctx, Skv)
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if ind == nil {
		t.Errorf("Nil index after Setbatch")
	}
	itList, err := s.GetBatchSV(ctx, &schema.KeyList{
		Keys: []*schema.Key{
			{
				Key: Skv.SKVs[0].Key,
			},
			{
				Key: Skv.SKVs[1].Key,
			},
			{
				Key: Skv.SKVs[2].Key,
			},
		},
	})
	for ind, val := range itList.Items {
		if !bytes.Equal(val.Value.Payload, Skv.SKVs[ind].Value.Payload) {
			t.Errorf("BatchSetSV value not equal to BatchGetSV value, expected %s, got %s", string(Skv.SKVs[ind].Value.Payload), string(val.Value.Payload))
		}
		if val.Value.Timestamp != Skv.SKVs[ind].Value.Timestamp {
			t.Errorf("BatchSetSV value not equal to BatchGetSV value, expected %d, got %d", Skv.SKVs[ind].Value.Timestamp, val.Value.Timestamp)
		}
	}
}
func testInclusion(ctx context.Context, s *ImmuServer, t *testing.T) {
	for _, val := range kv {
		_, err := s.Set(ctx, val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
	}
	ind := uint64(1)
	inc, err := s.Inclusion(ctx, &schema.Index{Index: ind})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if inc.Index != ind {
		t.Errorf("Inclusion, expected %d, got %d", inc.Index, ind)
	}
}
func testConsintency(ctx context.Context, s *ImmuServer, t *testing.T) {
	for _, val := range kv {
		_, err := s.Set(ctx, val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
	}
	ind := uint64(1)
	inc, err := s.Consistency(ctx, &schema.Index{Index: ind})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if inc.First != ind {
		t.Errorf("Consistency, expected %d, got %d", inc.First, ind)
	}
}

func testByIndex(ctx context.Context, s *ImmuServer, t *testing.T) {

	ind := uint64(1)
	for _, val := range kv {
		it, err := s.Set(ctx, val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		ind = it.Index
	}
	inc, err := s.ByIndex(ctx, &schema.Index{Index: ind})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if !bytes.Equal(inc.Value, kv[len(kv)-1].Value) {
		t.Errorf("ByIndex, expected %s, got %d", kv[ind].Value, inc.Value)
	}
}

func testByIndexSV(ctx context.Context, s *ImmuServer, t *testing.T) {
	ind := uint64(2)
	for _, val := range Skv.SKVs {
		it, err := s.SetSV(ctx, val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		ind = it.Index
	}
	time.Sleep(1 * time.Second)
	inc, err := s.ByIndexSV(ctx, &schema.Index{Index: ind})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if !bytes.Equal(inc.Value.Payload, Skv.SKVs[len(Skv.SKVs)-1].Value.Payload) {
		t.Errorf("ByIndexSV, expected %s, got %s", string(Skv.SKVs[len(Skv.SKVs)-1].Value.Payload), string(inc.Value.Payload))
	}
}

func testBySafeIndex(ctx context.Context, s *ImmuServer, t *testing.T) {
	for _, val := range Skv.SKVs {
		_, err := s.SetSV(ctx, val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
	}
	time.Sleep(1 * time.Second)
	ind := uint64(1)
	inc, err := s.BySafeIndex(ctx, &schema.SafeIndexOptions{Index: ind})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if inc.Item.Index != ind {
		t.Errorf("ByIndexSV, expected %d, got %d", ind, inc.Item.Index)
	}
}

func testHistory(ctx context.Context, s *ImmuServer, t *testing.T) {
	inc, err := s.History(ctx, &schema.Key{
		Key: testKey,
	})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	for _, val := range inc.Items {
		if !bytes.Equal(val.Value, testValue) {
			t.Errorf("History, expected %s, got %s", val.Value, testValue)
		}
	}
}

func testHistorySV(ctx context.Context, s *ImmuServer, t *testing.T) {
	k := &schema.Key{
		Key: testValue,
	}
	items, err := s.HistorySV(ctx, k)
	if err != nil {
		t.Errorf("Error reading key %s", err)
	}
	for _, val := range items.Items {
		if !bytes.Equal(val.Value.Payload, testValue) {
			t.Errorf("HistorySV, expected %s, got %s", testValue, val.Value.Payload)
		}
	}
}

func testHealth(ctx context.Context, s *ImmuServer, t *testing.T) {
	h, err := s.Health(ctx, &emptypb.Empty{})
	if err != nil {
		t.Errorf("health error %s", err)
	}
	if !h.GetStatus() {
		t.Errorf("Health, expected %v, got %v", true, h.GetStatus())
	}
}

func testReference(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.Set(ctx, kv[0])
	if err != nil {
		t.Errorf("Reference error %s", err)
	}
	_, err = s.Reference(ctx, &schema.ReferenceOptions{
		Reference: []byte(`tag`),
		Key:       kv[0].Key,
	})
	item, err := s.Get(ctx, &schema.Key{Key: []byte(`tag`)})
	if err != nil {
		t.Errorf("Reference  Get error %s", err)
	}
	if !bytes.Equal(item.Value, kv[0].Value) {
		t.Errorf("Reference, expected %v, got %v", string(item.Value), string(kv[0].Value))
	}
}

func testZAdd(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.Set(ctx, kv[0])
	if err != nil {
		t.Errorf("Reference error %s", err)
	}
	_, err = s.ZAdd(ctx, &schema.ZAddOptions{
		Key:   kv[0].Key,
		Score: 1,
		Set:   kv[0].Value,
	})
	item, err := s.ZScan(ctx, &schema.ZScanOptions{
		Offset:  []byte(""),
		Limit:   3,
		Reverse: false,
	})
	if err != nil {
		t.Errorf("Reference  Get error %s", err)
	}
	if !bytes.Equal(item.Items[0].Value, kv[0].Value) {
		t.Errorf("Reference, expected %v, got %v", string(kv[0].Value), string(item.Items[0].Value))
	}
}

func testScan(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.Set(ctx, kv[0])
	if err != nil {
		t.Errorf("set error %s", err)
	}
	_, err = s.ZAdd(ctx, &schema.ZAddOptions{
		Key:   kv[0].Key,
		Score: 3,
		Set:   kv[0].Value,
	})
	if err != nil {
		t.Errorf("zadd error %s", err)
	}

	_, err = s.SafeZAdd(ctx, &schema.SafeZAddOptions{
		Zopts: &schema.ZAddOptions{
			Key:   kv[0].Key,
			Score: 0,
			Set:   kv[0].Value,
		},
		RootIndex: &schema.Index{
			Index: 0,
		},
	})
	if err != nil {
		t.Errorf("SafeZAdd error %s", err)
	}

	item, err := s.Scan(ctx, &schema.ScanOptions{
		Offset: nil,
		Deep:   false,
		Limit:  1,
		Prefix: kv[0].Key,
	})

	if err != nil {
		t.Errorf("ZScanSV  Get error %s", err)
	}
	if !bytes.Equal(item.Items[0].Key, kv[0].Key) {
		t.Errorf("Reference, expected %v, got %v", string(kv[0].Key), string(item.Items[0].Key))
	}

	scanItem, err := s.IScan(ctx, &schema.IScanOptions{
		PageNumber: 2,
		PageSize:   1,
	})
	if err != nil {
		t.Errorf("ZScanSV  Get error %s", err)
	}
	if !bytes.Equal(scanItem.Items[0].Key, kv[0].Key) {
		t.Errorf("Reference, expected %v, got %v", string(kv[0].Key), string(scanItem.Items[0].Value))
	}
}

func TestUsermanagement(t *testing.T) {
	var err error
	s := newInmemoryAuthServer()
	ctx, err := loginSysAdmin(s)
	if err != nil {
		log.Fatal(err)
	}
	newdb := &schema.Database{
		Databasename: testDatabase,
	}
	dbrepl, err := s.CreateDatabase(ctx, newdb)
	if err != nil {
		log.Fatal(err)
	}
	if dbrepl.Error.Errorcode != schema.ErrorCodes_Ok {
		log.Fatalf("error creating test database %v", dbrepl)
	}

	testCreateUser(ctx, s, t)
	testListDatabases(ctx, s, t)
	testUseDatabase(ctx, s, t)
	testChangePermission(ctx, s, t)
	testDeactivateUser(ctx, s, t)
	testSetActiveUser(ctx, s, t)
	testChangePassword(ctx, s, t)
}
func TestDbOperations(t *testing.T) {
	var err error
	s := newInmemoryAuthServer()
	ctx, err := loginSysAdmin(s)
	if err != nil {
		log.Fatal(err)
	}
	newdb := &schema.Database{
		Databasename: testDatabase,
	}
	dbrepl, err := s.CreateDatabase(ctx, newdb)
	if err != nil {
		log.Fatal(err)
	}
	if dbrepl.Error.Errorcode != schema.ErrorCodes_Ok {
		log.Fatalf("error creating test database %v", dbrepl)
	}
	testSetGet(ctx, s, t)
	testSVSetGet(ctx, s, t)
	testCurrentRoot(ctx, s, t)
	testSafeSetGet(ctx, s, t)
	testSafeSetGetSV(ctx, s, t)
	testSetGetBatch(ctx, s, t)
	testSetGetBatchSV(ctx, s, t)
	testInclusion(ctx, s, t)
	testConsintency(ctx, s, t)
	testByIndexSV(ctx, s, t)
	testByIndex(ctx, s, t)
	testHistory(ctx, s, t)
	testBySafeIndex(ctx, s, t)
	testHealth(ctx, s, t)
	testHistorySV(ctx, s, t)
	testReference(ctx, s, t)
	testZAdd(ctx, s, t)
	testScan(ctx, s, t)
}
