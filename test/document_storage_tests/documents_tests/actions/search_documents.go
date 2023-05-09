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
	"fmt"
	"net/http"

	"github.com/gavv/httpexpect/v2"
)

func SearchDocuments(expect *httpexpect.Expect, sessionID string, collectionName string, payload map[string]interface{}) *httpexpect.Object {
	document := expect.POST(fmt.Sprintf("/collection/%s/documents/search", collectionName)).
		WithHeader("grpc-metadata-sessionid", sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().
		Value("revisions").Array().First().
		Object().Value("document").Object()

	return document
}
