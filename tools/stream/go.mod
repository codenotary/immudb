module streamer

go 1.13

require (
	github.com/codenotary/immudb v0.9.1
	github.com/mitchellh/go-homedir v1.1.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.7.0
	google.golang.org/grpc v1.37.0
)

replace github.com/codenotary/immudb v0.9.1 => ../../
