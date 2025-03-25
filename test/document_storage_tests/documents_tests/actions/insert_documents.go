/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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

	"github.com/codenotary/immudb/test/document_storage_tests/documents_tests/models"
	"github.com/gavv/httpexpect/v2"
)

func InsertOneDocumentWithMultipleFields(expect *httpexpect.Expect, sessionID string, collection *httpexpect.Object) models.Employee {
	collectionName := collection.Value("collection").Object().Value("name").String().Raw()

	document := models.Employee{
		BirthDate: "1964-06-02",
		FirstName: "Bezalel",
		LastName:  "Simmel",
		Gender:    "F",
		HireDate:  "1985-11-21",
	}

	payload := map[string]interface{}{
		"documents": []interface{}{
			document,
		},
	}

	insertDocuments(expect, sessionID, collectionName, payload, "first_name", document.FirstName)

	return document
}

func insertDocuments(expect *httpexpect.Expect, sessionID string, collectionName string, payload map[string]interface{}, field string, value interface{}) {
	expect.POST(fmt.Sprintf("/collection/%s/documents", collectionName)).
		WithHeader("grpc-metadata-sessionid", sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().NotEmpty().
		Keys().ContainsOnly("transactionId", "documentIds")
}
