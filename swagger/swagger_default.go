//go:build !swagger
// +build !swagger

package swagger

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/codenotary/immudb/embedded/logger"
)

//go:embed default/*
var content embed.FS

func SetupSwaggerUI(mux *http.ServeMux, l logger.Logger, addr string) error {
	fSys, err := fs.Sub(content, "default")
	if err != nil {
		return err
	}
	l.Infof("Swagger not built-in")
	mux.HandleFunc("/api/docs/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/missingswagger/", http.StatusTemporaryRedirect)
	})
	mux.Handle("/missingswagger/", http.StripPrefix("/missingswagger", http.FileServer(http.FS(fSys))))
	return nil
}
