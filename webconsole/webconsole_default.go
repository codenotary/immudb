//go:build !webconsole
// +build !webconsole

package webconsole

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/codenotary/immudb/embedded/logger"
)

//go:embed default/*
var content embed.FS

func SetupWebconsole(mux *http.ServeMux, l logger.Logger, addr string) error {
	fSys, err := fs.Sub(content, "default")
	if err != nil {
		return err
	}
	l.Infof("webconsole not built-in")
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/missing/", http.StatusTemporaryRedirect)
	})
	mux.Handle("/missing/", http.FileServer(http.FS(fSys)))
	return nil
}
