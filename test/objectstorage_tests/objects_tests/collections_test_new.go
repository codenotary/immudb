package main

import (
	"net/http"
	"os"
	"testing"

	"github.com/gavv/httpexpect/v2"
)

func TestCreateCollectionWithInvalidName(t *testing.T) {
	var baseURL = os.Getenv("OBJECTS_TEST_BASEURL")
	var password = os.Getenv("OBJECTS_TEST_PASSWORD")

	user := map[string]interface{}{
		"username": "immudb",
		"password": password,
		"database": "defaultdb",
	}
	e := httpexpect.Default(t, baseURL)
	obj := e.POST("/authorization/session/open").
		WithJSON(user).
		Expect().
		Status(http.StatusOK).JSON().Object()
	token := obj.Value("token").Raw().(string)

	e.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", token).
		WithQuery("name", "accounts").
		Expect().
		Status(http.StatusOK).JSON().Object().NotEmpty()
}
