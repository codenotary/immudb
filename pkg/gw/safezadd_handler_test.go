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
package gw

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/auth"
	immuclient "github.com/codenotary/immudb/pkg/client"
	immudb "github.com/codenotary/immudb/pkg/server"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

var safezaddHandlerTestDir = "./safezadd_handler_test"

func TestSafeZAdd(t *testing.T) {
	setName := base64.StdEncoding.EncodeToString([]byte("Soprano"))
	uknownKey := base64.StdEncoding.EncodeToString([]byte("Marias Callas"))
	tcpPort := generateRandomTCPPort()
	//MetricsServer must not be started as during tests because prometheus lib panics with: duplicate metrics collector registration attempted
	op := immudb.DefaultOptions().
		WithPort(tcpPort).WithDir("db_" + strconv.FormatInt(int64(tcpPort), 10)).
		WithMetricsServer(false).WithCorruptionCheck(false).WithAuth(false)
	s := immudb.DefaultServer().WithOptions(op)
	go s.Start()
	time.Sleep(2 * time.Second)
	defer func() {
		s.Stop()
		time.Sleep(2 * time.Second) //without the delay the db dir is deleted before all the data has been flushed to disk and results in crash.
		os.RemoveAll(op.GetDataDir())
		os.RemoveAll(safezaddHandlerTestDir)
	}()

	ic, err := immuclient.NewImmuClient(immuclient.DefaultOptions().
		WithPort(tcpPort).WithDir(safezaddHandlerTestDir))

	if err != nil {
		t.Errorf("unable to instantiate client: %s", err)
		return
	}
	item, err := ic.Login(context.Background(), []byte(auth.AdminUsername), []byte(auth.AdminDefaultPassword))
	if err != nil {
		t.Errorf("%s", err)
	}
	token := item.GetToken()
	refkey, err := insertSampleSet(ic, safezaddHandlerTestDir, token)
	if err != nil {
		t.Errorf("%s", err)
	}
	mux := runtime.NewServeMux()
	ssh := NewSafeZAddHandler(mux, ic)

	tt := []struct {
		test           string
		payload        string
		wantStatus     int
		wantFieldName  string
		wantFieldValue interface{}
	}{
		{
			"Send correct request",
			`{
				"zopts": {
					"set":  "` + setName + `",
					"score": 1.0,
					"key": "` + refkey + `"
				}
			}
			`,
			http.StatusOK,
			"verified",
			true,
		},
		{
			"Send correct requestm with oknown key",
			`{
				"zopts": {
					"set":  "` + setName + `",
					"score": 1.0,
					"key": "` + uknownKey + `"
				}
			}
			`,
			http.StatusOK,
			"error",
			"Key not found",
		},
		{
			"Send incorrect json field",
			`{
				"zoptsi": {
					"set":  "` + setName + `",
					"score": 1.0,
					"key": "` + setName + `"
				}
			}`,
			http.StatusBadRequest,
			"error",
			"incorrect JSON payload",
		},
		{
			"Missing Key field",
			`{
				"zopts": {
					"set":  "` + setName + `",
					"score": 1.0
				}
			}
			`,
			http.StatusBadRequest,
			"error",
			"invalid key",
		},
		{
			"Send ASCII instead of base64 encoded",
			`{
				"zopts": {
					"set":  "` + setName + `",
					"score": 1.0,
					"key": "Diana Damrau"
				}
			}
			`,
			http.StatusBadRequest,
			"error",
			"illegal base64 data at input byte 5",
		},
	}

	for _, tc := range tt {
		t.Run(tc.test, func(t *testing.T) {
			w := httptest.NewRecorder()
			req, _ := http.NewRequest("POST", "/v1/immurestproxy/safe/zadd", strings.NewReader(tc.payload))
			req.Header.Add("Content-Type", "application/json")

			safeset := func(res http.ResponseWriter, req *http.Request) {
				ssh.SafeZAdd(res, req, nil)
			}
			handler := http.HandlerFunc(safeset)
			handler.ServeHTTP(w, req)
			var body map[string]interface{}

			err = json.Unmarshal(w.Body.Bytes(), &body)
			if err != nil {
				t.Error("bad reply JSON")
			}
			fieldValue, ok := body[tc.wantFieldName]
			if !ok {
				t.Errorf("json reply required field not found")
			}
			if fieldValue != tc.wantFieldValue {
				t.Errorf("handler returned wrong json reply: got %v want %v",
					fieldValue, tc.wantFieldValue)
				t.Error(body)
				t.Error(string(w.Body.Bytes()))
			}

			// TODO gjergji this should be used once #263 is fixed
			// if w.Code != tc.want {
			// 	t.Errorf("handler returned wrong status code: got %v want %v",
			// 		w.Code, tc.want)
			// }
		})
	}
}
