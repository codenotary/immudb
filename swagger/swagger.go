//go:build swagger
// +build swagger

package swagger

//go:generate go run github.com/rakyll/statik -f -src=./dist -p=swaggerembedded -dest=. -tags=swagger

import (
	"net/http"

	_ "github.com/codenotary/immudb/swagger/swaggerembedded"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/rakyll/statik/fs"
)

var statikFS, err = fs.New()

func SetupSwaggerUI(mux *http.ServeMux, l logger.Logger, addr string) error {
	if err != nil {
		return err
	}
	l.Infof("Swagger UI enabled: %s", addr)
	mux.Handle("/api/docs/", http.StripPrefix("/api/docs", http.FileServer(statikFS)))
	return nil
}
