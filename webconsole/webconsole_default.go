//go:build !webconsole
// +build !webconsole

package webconsole

//go:generate go run github.com/rakyll/statik -f -src=./default -p=webconsole -dest=. -tags=!webconsole

import (
	"net/http"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/rakyll/statik/fs"
)

var statikFS, err = fs.New()

func SetupWebconsole(mux *http.ServeMux, l logger.Logger, addr string) error {
	if err != nil {
		return err
	}
	l.Infof("Webconsole not built-in")
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/missing/", http.StatusTemporaryRedirect)
	})
	mux.Handle("/missing/", http.FileServer(statikFS))
	return nil
}
