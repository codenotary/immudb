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
	"encoding/base64"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	immuclient "github.com/codenotary/immudb/pkg/client"
	immudb "github.com/codenotary/immudb/pkg/server"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

func generateRandomTCPPort() int {
	rand.Seed(time.Now().UnixNano())
	min := 1024
	max := 64000
	return rand.Intn(max-min+1) + min
}

func TestSafeset(t *testing.T) {
	key := base64.StdEncoding.EncodeToString([]byte("Pablo"))
	value := base64.StdEncoding.EncodeToString([]byte("Picasso"))
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
		os.RemoveAll(op.Dir)
	}()

	ic, err := immuclient.NewImmuClient(immuclient.DefaultOptions().WithPort(tcpPort))
	if err != nil {
		t.Errorf("unable to instantiate client: %s", err)
		return
	}
	mux := runtime.NewServeMux()
	ssh := NewSafesetHandler(mux, ic)

	tt := []struct {
		test           string
		payload        string
		wantStatus     int
		wantFieldName  string
		wantFieldValue interface{}
	}{
		{ //Pablo = UGFibG8=
			//Picasso = UGljYXNzbw==
			"Send correct request",
			`{"kv": {"key": "` + key + `", "value": "` + value + `"	} }`,
			http.StatusOK,
			"verified",
			true,
		},
		{
			"Missing value field",
			`{"kv": {"key": "UGFibG8="} }`,
			http.StatusOK,
			"verified",
			true,
		},
		{
			"Send incorrect json field",
			`{"data": {"key": "` + key + `", "value": "` + value + `"} }`,
			http.StatusBadRequest,
			"error",
			"incorrect JSON payload",
		},
		{
			"Send ASCII instead of base64 encoded",
			`{"kv": {"key": "Pablo", "value": "Picasso" } }`,
			http.StatusBadRequest,
			"error",
			"illegal base64 data at input byte 4",
		},
		{
			"Missing key field",
			`{"kv": {} }`,
			http.StatusBadRequest,
			"error",
			"invalid key",
		},
	}

	for _, tc := range tt {
		t.Run(tc.test, func(t *testing.T) {
			w := httptest.NewRecorder()
			req, _ := http.NewRequest("POST", "/v1/immurestproxy/item/safe", strings.NewReader(tc.payload))
			req.Header.Add("Content-Type", "application/json")

			safeset := func(res http.ResponseWriter, req *http.Request) {
				ssh.Safeset(res, req, nil)
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
