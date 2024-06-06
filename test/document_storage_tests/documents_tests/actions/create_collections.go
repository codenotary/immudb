/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

	"github.com/gavv/httpexpect/v2"
)

func CreateCollectionWithName(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	return createCollection(expect, sessionID, name, nil)
}

func CreateCollectionWithNameAndOneField(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"fields": []interface{}{
			map[string]interface{}{
				"name": "first_name",
				"type": "STRING",
			},
		},
	}

	return createCollection(expect, sessionID, name, payload)
}

func CreateCollectionWithNameAndIdFieldName(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"documentIdFieldName": "emp_no",
	}

	return createCollection(expect, sessionID, name, payload)
}

func CreateCollectionWithNameIdFieldNameAndOneField(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"documentIdFieldName": "emp_no",
		"fields": []interface{}{
			map[string]interface{}{
				"name": "hire_date",
				"type": "STRING",
			},
		},
	}

	return createCollection(expect, sessionID, name, payload)
}

func CreateCollectionWithNameOneFieldAndOneUniqueIndex(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"fields": []interface{}{
			map[string]interface{}{
				"name": "id_number",
				"type": "INTEGER",
			},
		},
		"indexes": []interface{}{
			map[string]interface{}{
				"fields": []string{
					"id_number",
				},
				"isUnique": true,
			},
		},
	}

	return createCollection(expect, sessionID, name, payload)
}

func CreateCollectionWithNameAndMultipleFields(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"fields": []interface{}{
			map[string]interface{}{
				"name": "birth_date",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "first_name",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "last_name",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "gender",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "hire_date",
				"type": "STRING",
			},
		},
	}

	return createCollection(expect, sessionID, name, payload)
}

func CreateCollectionWithNameMultipleFieldsAndMultipleIndexes(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"fields": []interface{}{
			map[string]interface{}{
				"name": "birth_date",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "first_name",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "last_name",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "gender",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "hire_date",
				"type": "STRING",
			},
		},
		"indexes": []interface{}{
			map[string]interface{}{
				"fields": []string{
					"birth_date", "last_name",
				},
				"isUnique": true,
			},
		},
	}

	return createCollection(expect, sessionID, name, payload)
}

func createCollection(expect *httpexpect.Expect, sessionID string, name string, payload map[string]interface{}) *httpexpect.Object {
	expect.POST(fmt.Sprintf("/collection/%s", name)).
		WithHeader("grpc-metadata-sessionid", sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().IsEmpty()

	collection := expect.GET(fmt.Sprintf("/collection/%s", name)).
		WithHeader("grpc-metadata-sessionid", sessionID).
		Expect().
		Status(http.StatusOK).
		JSON().Object()

	return collection
}
