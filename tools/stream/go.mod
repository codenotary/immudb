module streamer

go 1.13

require (
	github.com/codenotary/immudb v0.9.1
	github.com/coreos/bbolt v1.3.2 // indirect
	github.com/coreos/etcd v3.3.13+incompatible // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/prometheus/tsdb v0.7.1 // indirect
	github.com/spf13/cobra v1.2.1
	github.com/spf13/viper v1.8.1
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	google.golang.org/grpc v1.39.0
)

replace github.com/codenotary/immudb v0.9.1 => ../../
