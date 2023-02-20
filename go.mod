module github.com/codenotary/immudb

go 1.15

require (
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/Masterminds/sprig v2.22.0+incompatible // indirect
	github.com/aead/chacha20poly1305 v0.0.0-20201124145622-1a5aba2a8b29 // indirect
	github.com/fatih/color v1.13.0
	github.com/gizak/termui/v3 v3.1.0
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/huandu/xstrings v1.3.2 // indirect
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/influxdata/influxdb-client-go/v2 v2.12.2
	github.com/jackc/pgx/v4 v4.16.1
	github.com/jaswdr/faker v1.4.3
	github.com/lib/pq v1.10.2
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/mattn/goveralls v0.0.11
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-wordwrap v1.0.1 // indirect
	github.com/nsf/termbox-go v1.1.1 // indirect
	github.com/o1egl/paseto v1.0.0
	github.com/olekukonko/tablewriter v0.0.5
	github.com/ory/go-acc v0.2.8
	github.com/peterh/liner v1.2.1
	github.com/prometheus/client_golang v1.12.2
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.32.1
	github.com/prometheus/procfs v0.7.3
	github.com/pseudomuto/protoc-gen-doc v1.4.1
	github.com/pseudomuto/protokit v0.2.1 // indirect
	github.com/rakyll/statik v0.1.7
	github.com/rogpeppe/go-internal v1.8.0
	github.com/rs/xid v1.3.0
	github.com/schollz/progressbar/v2 v2.15.0
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.12.0
	github.com/stretchr/testify v1.8.0
	github.com/takama/daemon v0.12.0
	golang.org/x/crypto v0.0.0-20220411220226-7b82a4e95df4
	golang.org/x/net v0.0.0-20220708220712-1185a9018129
	golang.org/x/sys v0.0.0-20220708085239-5a0f0661e09d
	golang.org/x/tools v0.1.5
	google.golang.org/genproto v0.0.0-20220519153652-3a47de7e79bd
	google.golang.org/grpc v1.46.2
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.1.0
	google.golang.org/protobuf v1.28.0
	gopkg.in/ini.v1 v1.66.6 // indirect
)

replace github.com/takama/daemon v0.12.0 => github.com/codenotary/daemon v0.0.0-20200507161650-3d4bcb5230f4

replace github.com/spf13/afero => github.com/spf13/afero v1.5.1

replace golang.org/x/crypto => golang.org/x/crypto v0.0.0-20220314234659-1baeb1ce4c0b
