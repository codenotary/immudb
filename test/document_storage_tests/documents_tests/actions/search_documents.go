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

func SearchDocuments(expect *httpexpect.Expect, sessionID string, collection *httpexpect.Object, fieldComparison models.FieldComparison) *httpexpect.Object {
	fieldComparisons := []models.FieldComparison{fieldComparison}
	expressions := models.Expressions{FieldComparisons: fieldComparisons}
	query := models.Query{Expressions: []models.Expressions{expressions}}
	payload := models.SearchPayload{
		Query:    query,
		Page:     1,
		PageSize: 1,
	}

	document := expect.POST(fmt.Sprintf("/collection/%s/documents/search", collection.Value("collection").Object().Value("name").String().Raw())).
		WithHeader("grpc-metadata-sessionid", sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().
		Value("revisions").Array().First().
		Object().Value("document").Object()

	return document
}
