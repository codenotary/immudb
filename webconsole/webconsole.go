//go:build webconsole
// +build webconsole

package webconsole

//go:generate go run github.com/rakyll/statik -f -src=./dist -p=webconsole -dest=../ -tags=webconsole

import (
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/rakyll/statik/fs"
	"net/http"
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
