//go:build swagger
// +build swagger

package swagger

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetupSwaggerUI(t *testing.T) {

	req, err := http.NewRequest("GET", "/api/docs/", nil)
	require.NoError(t, err)

	handler := http.NewServeMux()
	SetupSwaggerUI(handler, logger.NewSimpleLogger("swagger", os.Stderr), "localhost:8080")
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.True(t, rr.Code == 200)

	// Test just if the response exist
	page, err := ioutil.ReadAll(rr.Body)
	require.NoError(t, err)

	assert.Contains(t, string(page), "<title>Swagger UI</title>")
}
