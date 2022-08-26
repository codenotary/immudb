//go:build !webconsole
// +build !webconsole

package webconsole

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetupWebconsoleDefault(t *testing.T) {

	req, err := http.NewRequest("GET", "/", nil)
	require.NoError(t, err)

	handler := http.NewServeMux()
	err = SetupWebconsole(handler, logger.NewSimpleLogger("webconsole", os.Stderr), "localhost:8080")
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusTemporaryRedirect, rr.Code)

	page, err := ioutil.ReadAll(rr.Body)
	require.NoError(t, err)

	assert.Contains(t, string(page), "missing")
}
