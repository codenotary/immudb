module github.com/codenotary/immudb

go 1.13

require (
	github.com/codenotary/immugw v0.0.0-20200828165640-ce49b70a5bfd
	github.com/codenotary/merkletree v0.1.2-0.20200720105344-68d95395a656
	github.com/dgraph-io/badger/v2 v2.0.0-20200408100755-2e708d968e94
	github.com/fatih/color v1.9.0
	github.com/gizak/termui/v3 v3.1.0
	github.com/golang/protobuf v1.4.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.14.4
	github.com/jaswdr/faker v1.0.2
	github.com/mitchellh/go-homedir v1.1.0
	github.com/o1egl/paseto v1.0.0
	github.com/peterh/liner v1.2.0
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/rs/xid v1.2.1
	github.com/spf13/cobra v1.0.0
	github.com/spf13/viper v1.6.3
	github.com/stretchr/testify v1.5.1
	github.com/takama/daemon v0.12.0
	golang.org/x/crypto v0.0.0-20200423211502-4bdfaf469ed5
	golang.org/x/sys v0.0.0-20200420163511-1957bb5e6d1f
	google.golang.org/genproto v0.0.0-20200424135956-bca184e23272
	google.golang.org/grpc v1.29.1
	google.golang.org/protobuf v1.21.0
)

replace github.com/takama/daemon v0.12.0 => github.com/codenotary/daemon v0.0.0-20200507161650-3d4bcb5230f4
