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

	"github.com/gavv/httpexpect/v2"
)

func CreateCollectionWithName(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"name": name,
	}

	return createCollection(expect, sessionID, payload)
}

func CreateCollectionWithNameAndOneField(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"name": name,
		"fields": []interface{}{
			map[string]interface{}{
				"name": "first_name",
				"type": "STRING",
			},
		},
	}

	return createCollection(expect, sessionID, payload)
}

func CreateCollectionWithNameAndIdFieldName(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"name":                name,
		"documentIdFieldName": "emp_no",
	}

	return createCollection(expect, sessionID, payload)
}

func CreateCollectionWithNameIdFieldNameAndOneField(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"name":                name,
		"documentIdFieldName": "emp_no",
		"fields": []interface{}{
			map[string]interface{}{
				"name": "hire_date",
				"type": "STRING",
			},
		},
	}

	return createCollection(expect, sessionID, payload)
}

func CreateCollectionWithNameOneFieldAndOneUniqueIndex(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"name": name,
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

	return createCollection(expect, sessionID, payload)
}

func CreateCollectionWithNameAndMultipleFields(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"name": name,
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

	return createCollection(expect, sessionID, payload)
}

func CreateCollectionWithNameMultipleFieldsAndMultipleIndexes(expect *httpexpect.Expect, sessionID string, name string) *httpexpect.Object {
	payload := map[string]interface{}{
		"name": name,
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

	return createCollection(expect, sessionID, payload)
}

func createCollection(expect *httpexpect.Expect, sessionID string, payload map[string]interface{}) *httpexpect.Object {
	expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().Empty()

	collection := expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", sessionID).
		WithQuery("name", payload["name"]).
		Expect().
		Status(http.StatusOK).
		JSON().Object()

	return collection
}
