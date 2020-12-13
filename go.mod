module github.com/codenotary/immudb

go 1.13

require (
	github.com/Masterminds/goutils v1.1.0 // indirect
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/Masterminds/sprig v2.22.0+incompatible // indirect
	github.com/aokoli/goutils v1.1.0 // indirect
	github.com/codenotary/merkletree v0.1.2-0.20200720105344-68d95395a656
	github.com/dgraph-io/badger/v2 v2.0.0-20200408100755-2e708d968e94
	github.com/envoyproxy/protoc-gen-validate v0.4.1 // indirect
	github.com/fatih/color v1.9.0
	github.com/gizak/termui/v3 v3.1.0
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/protobuf v1.4.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/huandu/xstrings v1.3.2 // indirect
	github.com/imdario/mergo v0.3.11 // indirect
	github.com/jaswdr/faker v1.0.2
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/reflectwalk v1.0.1 // indirect
	github.com/mwitkow/go-proto-validators v0.3.2 // indirect
	github.com/o1egl/paseto v1.0.0
	github.com/peterh/liner v1.2.0
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/pseudomuto/protoc-gen-doc v1.3.2 // indirect
	github.com/rs/xid v1.2.1
	github.com/spf13/cobra v1.0.0
	github.com/spf13/viper v1.6.3
	github.com/stretchr/testify v1.6.1
	github.com/takama/daemon v0.12.0
	golang.org/x/crypto v0.0.0-20201208171446-5f87f3452ae9
	golang.org/x/net v0.0.0-20201209123823-ac852fbbde11 // indirect
	golang.org/x/sys v0.0.0-20201207223542-d4d67f95c62d
	golang.org/x/text v0.3.4 // indirect
	google.golang.org/genproto v0.0.0-20201207150747-9ee31aac76e7
	google.golang.org/grpc v1.34.0
	google.golang.org/protobuf v1.25.0
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/takama/daemon v0.12.0 => github.com/codenotary/daemon v0.0.0-20200507161650-3d4bcb5230f4
