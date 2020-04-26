package commands

import (
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/viper"
)

func (cl *commandline) options() *client.Options {
	port := viper.GetInt("default.port")
	address := viper.GetString("default.address")
	authEnabled := viper.GetBool("default.auth")
	mtls := viper.GetBool("default.mtls")
	certificate := viper.GetString("default.certificate")
	servername := viper.GetString("default.servername")
	pkey := viper.GetString("default.pkey")
	clientcas := viper.GetString("default.clientcas")
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithAuth(authEnabled).
		WithMTLs(mtls)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = client.DefaultMTLsOptions().
			WithServername(servername).
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(clientcas)
	}
	return options
}
