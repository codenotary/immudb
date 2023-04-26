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

	id := uuid.New()
	documentId := id.String()

	documentToInsert := make(map[string]interface{})
	documentToInsert["_id"] = id.String()
	documentToInsert["name"] = "John"
	documentToInsert["surname"] = "Doe"
	documentToInsert["age"] = 30

	collection, err := createRandomCollection(client)
	require.NoError(t, err)

	req := httpclient.ModelDocumentInsertRequest{
		Collection: collection.Name,
		Document:   &documentToInsert,
	}
	response, _ := client.DocumentInsertWithResponse(context.Background(), req)
	require.True(t, response.StatusCode() == 200)

	fieldName := "_id"
	operator := httpclient.EQ

	query := httpclient.ModelQuery{
		Expressions: &[]httpclient.ModelQueryExpression{
			{
				FieldComparisons: &[]httpclient.ModelFieldComparison{
					{
						Field:    &fieldName,
						Operator: &operator,
						Value: &map[string]interface{}{
							"_id": &documentId,
						},
					},
				},
			},
		},
	}

	page := int64(1)
	perPage := int64(100)

	searchReq := httpclient.ModelDocumentSearchRequest{
		Collection: collection.Name,
		Query:      &query,
		Page:       &page,
		PerPage:    &perPage,
	}

	searchResponse, _ := client.DocumentSearchWithResponse(context.Background(), searchReq)
	fmt.Println(searchResponse.StatusCode())
	require.True(t, searchResponse.StatusCode() == 200)

	revisions := *searchResponse.JSON200.Revisions

	firstDocument := (*revisions[0].Document)

	require.True(t, firstDocument["_id"] == documentId)
	require.True(t, firstDocument["age"] == 30)
	require.True(t, firstDocument["name"] == "John")
	require.True(t, firstDocument["surname"] == "Doe")

}
