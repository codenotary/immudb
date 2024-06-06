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

package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/codenotary/immudb/pkg/api/httpclient"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCreateDocument(t *testing.T) {
	client := getAuthorizedClient()

	uuid := uuid.New()

	documentToInsert := make(map[string]interface{})
	documentToInsert["uuid"] = uuid.String()
	documentToInsert["name"] = "John"
	documentToInsert["surname"] = "Doe"
	documentToInsert["age"] = 30

	collectionName := getStandarizedRandomString()

	err := createCollection(collectionName, client)
	require.NoError(t, err)

	req := httpclient.ModelDocumentInsertRequest{
		Collection: &collectionName,
		Document:   &documentToInsert,
	}
	response, err := client.DocumentInsertWithResponse(context.Background(), req)
	require.NoError(t, err)
	require.True(t, response.StatusCode() == 200)

	documentId := response.JSON200.DocumentId

	fieldName := "_id"
	operator := httpclient.EQ

	query := httpclient.ModelQuery{
		Expressions: &[]httpclient.ModelQueryExpression{
			{
				FieldComparisons: &[]httpclient.ModelFieldComparison{
					{
						Field:    &fieldName,
						Operator: &operator,
						Value:    documentId,
					},
				},
			},
		},
	}

	page := int64(1)
	perPage := int64(1)

	searchReq := httpclient.ModelDocumentSearchRequest{
		Collection: &collectionName,
		Query:      &query,
		Page:       &page,
		PerPage:    &perPage,
	}

	searchResponse, err := client.DocumentSearchWithResponse(context.Background(), searchReq)
	require.NoError(t, err)
	fmt.Println(searchResponse.StatusCode())
	require.True(t, searchResponse.StatusCode() == 200)

	revisions := *searchResponse.JSON200.Revisions

	firstDocument := (*revisions[0].Document)

	require.Equal(t, *documentId, firstDocument["_id"])
	require.Equal(t, float64(30), firstDocument["age"])
	require.Equal(t, "John", firstDocument["name"])
	require.Equal(t, "Doe", firstDocument["surname"])

}
