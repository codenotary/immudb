module github.com/codenotary/immudb

go 1.13

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/beevik/ntp v0.3.0
	github.com/codenotary/merkletree v0.1.1
	github.com/dgraph-io/badger/v2 v2.0.0-20200408100755-2e708d968e94
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/gizak/termui/v3 v3.1.0
	github.com/golang/protobuf v1.4.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.14.4
	github.com/jaswdr/faker v1.0.2
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/go-wordwrap v1.0.0 // indirect
	github.com/nsf/termbox-go v0.0.0-20200418040025-38ba6e5628f1 // indirect
	github.com/o1egl/paseto v1.0.0
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/rs/cors v1.7.0
	github.com/rs/xid v1.2.1
	github.com/spf13/cobra v1.0.0
	github.com/spf13/viper v1.6.3
	github.com/stretchr/testify v1.5.1
	github.com/takama/daemon v0.12.0
	golang.org/x/crypto v0.0.0-20200423211502-4bdfaf469ed5
	golang.org/x/net v0.0.0-20200425230154-ff2c4b7c35a0 // indirect
	golang.org/x/sys v0.0.0-20200420163511-1957bb5e6d1f
	golang.org/x/text v0.3.2 // indirect
	google.golang.org/genproto v0.0.0-20200424135956-bca184e23272
	google.golang.org/grpc v1.29.1
	gopkg.in/yaml.v2 v2.2.8 // indirect
)

replace github.com/takama/daemon v0.12.0 => github.com/codenotary/daemon v0.0.0-20200507161650-3d4bcb5230f4
