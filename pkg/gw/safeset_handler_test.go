package gw

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	immuclient "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/lithammer/shortuuid"
)

func TestSafeset(t *testing.T) {
	op := server.DefaultOptions()
	op.Dir = "db_" + shortuuid.New()
	s := server.
		DefaultServer().WithOptions(op)
	go s.Start()
	time.Sleep(2 * time.Second)
	defer s.Stop()
	defer os.RemoveAll(op.Dir)

	ic, err := immuclient.NewImmuClient(immuclient.DefaultOptions())
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
			"Send corrent request",
			`{"kv": {"key": "UGFibG8=", "value": "UGljYXNzbw=="	} }`,
			200,
			"verified",
			true,
		},
		{
			"Missing value field",
			`{"kv": {"key": "UGFibG8="} }`,
			200,
			"verified",
			true,
		},
		{
			"Send incorrent json field",
			`{"data": {"key": "UGFibG8=", "value": "UGljYXNzbw==" } }`,
			400,
			"error",
			"incorrect JSON payload",
		},
		{
			"Send ASCII instead of base64 encoded",
			`{"kv": {"key": "Pablo", "value": "Picasso" } }`,
			400,
			"error",
			"illegal base64 data at input byte 4",
		},
		{
			"Missing key field",
			`{"kv": {} }`,
			400,
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

			// if w.Code != tc.want {
			// 	t.Errorf("handler returned wrong status code: got %v want %v",
			// 		w.Code, tc.want)
			// }
		})
	}
}
