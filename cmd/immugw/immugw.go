package main

import (
	"fmt"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/spf13/cobra"
	"os"
)

func main() {

	immugwCmd := &cobra.Command{
		Use:   "immugw",
		Short: "Immu gateway",
		Long:  `Immu gateway is an intelligent proxy for immudb. It expose all gRPC methods with a rest interface and wrap SAFESET and SAFEGET endpoints with a verification service.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return serve(cmd, args)
		},
	}

	immugwCmd.Flags().IntP("port", "p", 8081, "immugw port number")
	immugwCmd.Flags().StringP("address", "s", "127.0.0.1", "immugw host address")
	immugwCmd.Flags().IntP("immudport", "j", 8080, "immudb port number")
	immugwCmd.Flags().StringP("immudaddress", "y", "127.0.0.1", "immudb host address")

	immugwCmd.Flags().BoolP("mtls", "m", false, "enable mutual tls")
	immugwCmd.Flags().String("servername", client.DefaultMTLsOptions().Servername, "used to verify the hostname on the returned certificates")
	immugwCmd.Flags().String("certificate", client.DefaultMTLsOptions().Certificate, "server certificate file path")
	immugwCmd.Flags().String("pkey", client.DefaultMTLsOptions().Pkey, "server private key path")
	immugwCmd.Flags().String("clientcas", client.DefaultMTLsOptions().ClientCAs, "clients certificates list. Aka certificate authority")

	if err := immugwCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serve(cmd *cobra.Command, args []string) error {
	port, err := cmd.Flags().GetInt("port")
	if err != nil {
		return err
	}
	address, err := cmd.Flags().GetString("address")
	if err != nil {
		return err
	}
	immudport, err := cmd.Flags().GetInt("immudport")
	if err != nil {
		return err
	}
	immudAddress, err := cmd.Flags().GetString("immudaddress")
	if err != nil {
		return err
	}
	mtls, err := cmd.Flags().GetBool("mtls")
	if err != nil {
		return err
	}
	servername, err := cmd.Flags().GetString("servername")
	if err != nil {
		return err
	}
	certificate, err := cmd.Flags().GetString("certificate")
	if err != nil {
		return err
	}
	pkey, err := cmd.Flags().GetString("pkey")
	if err != nil {
		return err
	}
	client_cas, err := cmd.Flags().GetString("clientcas")
	if err != nil {
		return err
	}
	options := gw.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithImmudAddress(immudAddress).
		WithImmudPort(immudport).
		WithMTLs(mtls)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = client.DefaultMTLsOptions().
			WithServername(servername).
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(client_cas)
	}
	immuGwServer := gw.
		DefaultServer().
		WithOptions(options)
	return immuGwServer.Start()
}
