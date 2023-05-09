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
	"reflect"

	"github.com/gavv/httpexpect/v2"
)

func InsertDocumentWithMultipleFields(expect *httpexpect.Expect, sessionID string, collection *httpexpect.Object, document map[string]interface{}) *httpexpect.Object {
	collectionName := collection.Value("collection").Object().Value("name").String().Raw()

	payload := map[string]interface{}{
		"collection": collectionName,
		"document":   document,
	}

	keys := reflect.ValueOf(document).MapKeys()
	field := keys[1].String()
	value := document[keys[1].String()]

	return insertDocument(expect, sessionID, payload, field, value)
}

func insertDocument(expect *httpexpect.Expect, sessionID string, payload map[string]interface{}, field string, value interface{}) *httpexpect.Object {
	expect.PUT("/documents/insert").
		WithHeader("grpc-metadata-sessionid", sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().NotEmpty().
		Keys().ContainsOnly("transactionId", "documentId")

	searchPayload := map[string]interface{}{
		"query": map[string]interface{}{
			"collection": payload["collection"],
			"expressions": []interface{}{
				map[string]interface{}{
					"fieldComparisons": []interface{}{
						map[string]interface{}{
							"field":    field,
							"operator": "EQ",
							"value":    value,
						},
					},
				},
			},
		},
		"page":     1,
		"pageSize": 1,
	}

	return SearchDocuments(expect, sessionID, searchPayload)
}
