
package webconsole

import (
	"embed"
)

//go:embed index.html nuxt_embedded images *.png *.ico
var FS embed.FS
