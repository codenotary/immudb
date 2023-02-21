//go:build webconsole
// +build webconsole

package webconsole

//go:generate go run github.com/rakyll/statik -f -src=./dist -p=webconsoleembedded -dest=. -tags=webconsole

import (
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/rakyll/statik/fs"
	"net/http"

	_ "github.com/codenotary/immudb/webconsole/webconsoleembedded"
)

var statikFS, err = fs.New()

func SetupWebconsole(mux *http.ServeMux, l logger.Logger, addr string) error {
	if err != nil {
		return err
	}
	l.Infof("Webconsole enabled: %s", addr)
	mux.Handle("/", http.FileServer(statikFS))
	return nil
}
