//go:build swagger
// +build swagger

package swagger

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/codenotary/immudb/embedded/logger"
)

//go:embed dist/*
var content embed.FS

func SetupSwaggerUI(mux *http.ServeMux, l logger.Logger, addr string) error {
	fSys, err := fs.Sub(content, "dist")
	if err != nil {
		return err
	}
	l.Infof("Swagger UI enabled: %s", addr)
	mux.Handle("/api/docs/", http.StripPrefix("/api/docs", http.FileServer(http.FS(fSys))))
	return nil
}
