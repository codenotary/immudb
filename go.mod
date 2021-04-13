module github.com/codenotary/immudb

go 1.16

require (
	github.com/fatih/color v1.9.0
	github.com/gizak/termui/v3 v3.1.0
	github.com/golang/protobuf v1.5.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/jaswdr/faker v1.0.2
	github.com/minio/minio-go/v7 v7.0.10
	github.com/o1egl/paseto v1.0.0
	github.com/olekukonko/tablewriter v0.0.5
	github.com/peterh/liner v1.2.0
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/pseudomuto/protoc-gen-doc v1.4.1
	github.com/rogpeppe/go-internal v1.6.2
	github.com/rs/xid v1.2.1
	github.com/spf13/afero v1.3.4 // indirect
	github.com/spf13/cobra v1.0.1-0.20201006035406-b97b5ead31f7
	github.com/spf13/viper v1.7.0
	github.com/stretchr/testify v1.7.0
	github.com/takama/daemon v0.12.0
	golang.org/x/crypto v0.0.0-20201208171446-5f87f3452ae9
	golang.org/x/net v0.0.0-20210119194325-5f4716e94777 // indirect
	golang.org/x/sys v0.0.0-20210124154548-22da62e12c0c
	golang.org/x/text v0.3.5 // indirect
	google.golang.org/genproto v0.0.0-20210224155714-063164c882e6
	google.golang.org/grpc v1.36.0
	google.golang.org/protobuf v1.26.0
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/takama/daemon v0.12.0 => github.com/codenotary/daemon v0.0.0-20200507161650-3d4bcb5230f4
