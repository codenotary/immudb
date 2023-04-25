/*
Copyright 2023 Codenotary Inc. All rights reserved.

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

package actions

import (
	"net/http"
	"os"
	"testing"

	"github.com/gavv/httpexpect/v2"
)

func OpenSession(t *testing.T) (*httpexpect.Expect, string) {
	baseURL, exists := os.LookupEnv("OBJECTS_TEST_BASEURL")

	if !exists {
		baseURL = "http://localhost:8091/api/v2"
	}

	user := map[string]interface{}{
		"username": "immudb",
		"password": "immudb",
		"database": "defaultdb",
	}

	expect := httpexpect.Default(t, baseURL)
	obj := expect.POST("/authorization/session/open").
		WithJSON(user).
		Expect().
		Status(http.StatusOK).JSON().Object()

	return expect, obj.Value("token").Raw().(string)
}
