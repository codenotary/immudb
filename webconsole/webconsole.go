//go:build webconsole
// +build webconsole

package webconsole

import (
	"embed"
	"github.com/codenotary/immudb/embedded/logger"
	"io/fs"
	"net/http"
)

//go:embed dist/*
var content embed.FS

func SetupWebconsole(mux *http.ServeMux, l logger.Logger, addr string) error {
	fSys, err := fs.Sub(content, "dist")
	if err != nil {
		return err
	}
	l.Infof("Webconsole enabled: %s", addr)
	mux.Handle("/", http.FileServer(http.FS(fSys)))
	return nil
}
