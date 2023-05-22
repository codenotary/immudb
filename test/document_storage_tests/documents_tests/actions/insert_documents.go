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

func InsertOneDocumentWithMultipleFields(expect *httpexpect.Expect, sessionID string, collection *httpexpect.Object) *httpexpect.Object {
	collectionName := collection.Value("collection").Object().Value("name").String().Raw()

	document := map[string]interface{}{
		"birth_date": "1964-06-02",
		"first_name": "Bezalel",
		"last_name":  "Simmel",
		"gender":     "F",
		"hire_date":  "1985-11-21",
	}

	payload := map[string]interface{}{
		"documents": []interface{}{
			document,
		},
	}

	return insertDocuments(expect, sessionID, collectionName, payload, "first_name", "Bezalel")
}

func insertDocuments(expect *httpexpect.Expect, sessionID string, collectionName string, payload map[string]interface{}, field string, value interface{}) *httpexpect.Object {
	expect.POST(fmt.Sprintf("/collection/%s/documents", collectionName)).
		WithHeader("grpc-metadata-sessionid", sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().NotEmpty().
		Keys().ContainsOnly("transactionId", "documentIds")

	searchPayload := map[string]interface{}{
		"query": map[string]interface{}{
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

	return SearchDocuments(expect, sessionID, collectionName, searchPayload)
}
