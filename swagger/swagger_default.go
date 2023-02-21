//go:build !swagger
// +build !swagger

package swagger

//go:generate go run github.com/rakyll/statik -f -src=./default -p=swaggerembedded -dest=. -tags=!swagger

import (
	"net/http"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/rakyll/statik/fs"

	_ "github.com/codenotary/immudb/swagger/swaggerembedded"
)

var statikFS, err = fs.New()

func SetupSwaggerUI(mux *http.ServeMux, l logger.Logger, addr string) error {
	if err != nil {
		return err
	}
	l.Infof("Swagger not built-in")
	mux.HandleFunc("/api/docs/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/missingswagger/", http.StatusTemporaryRedirect)
	})
	mux.Handle("/missingswagger/", http.StripPrefix("/missingswagger", http.FileServer(statikFS)))
	return nil
}
