// +build webconsole

package webconsole

//go:generate go run github.com/rakyll/statik -f -src=./dist -p=webconsole -dest=../ -tags=webconsole

import (
	"github.com/codenotary/immudb/pkg/logger"
	"net/http"
	"github.com/rakyll/statik/fs"
)

func SetupWebconsole(mux *http.ServeMux, l logger.Logger, addr string) error {
	statikFS, err := fs.New()
	if err != nil {
		return err
	}
	l.Infof("Webconsole enabled: %s", addr)
	mux.Handle("/", http.FileServer(statikFS))
	return nil
}
