// +build !webconsole

package webconsole

import (
	"github.com/codenotary/immudb/pkg/logger"
	"net/http"
)

func SetupWebconsole(_ *http.ServeMux, _ logger.Logger, _ string) error {
	// no op
	return nil
}
