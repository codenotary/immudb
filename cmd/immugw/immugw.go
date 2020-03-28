package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	rp "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/rs/cors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
)

func main() {

	immugwCmd := &cobra.Command{
		Use:   "immugw",
		Short: "Immu gateway",
		Long:  `Immu gateway is an intelligent proxy for immudb. It expose all gRPC methods with a rest interface and wrap SAFESET and SAFEGET endpoints with a verification service.`,
		Run: func(cmd *cobra.Command, args []string) {
			serve(cmd, args)
		},
	}

	immugwCmd.Flags().IntP("port", "p", 8081, "immugw port number")
	immugwCmd.Flags().StringP("host", "s", "127.0.0.1", "immugw host address")
	immugwCmd.Flags().IntP("immudport", "j", 8080, "immudb port number")
	immugwCmd.Flags().StringP("immudhost", "y", "127.0.0.1", "immudb host address")

	immugwCmd.Flags().BoolP("mtls", "m", false, "enable mutual tls")
	immugwCmd.Flags().String("certificate", client.DefaultMTLsOptions().Certificate, "server certificate file path")
	immugwCmd.Flags().String("pkey", client.DefaultMTLsOptions().Pkey, "server private key path")
	immugwCmd.Flags().String("clientcas", client.DefaultMTLsOptions().ClientCAs, "clients certificates list. Aka certificate authority")

	if err := immugwCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serve(cmd *cobra.Command, args []string) error {
	opts := []grpc.DialOption{grpc.WithInsecure()}

	port, err := cmd.Flags().GetInt("port")
	if err != nil {
		return err
	}
	host, err := cmd.Flags().GetString("host")
	if err != nil {
		return err
	}
	immudport, err := cmd.Flags().GetInt("immudport")
	if err != nil {
		return err
	}
	immudhost, err := cmd.Flags().GetString("immudhost")
	if err != nil {
		return err
	}
	mtls, err := cmd.Flags().GetBool("mtls")
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
	options := client.DefaultOptions().
		WithAddress(immudhost).
		WithPort(immudport).
		WithMTLs(mtls)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = client.DefaultMTLsOptions().
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(client_cas)
	}

	//---------- TLS Setting -----------//
	if options.MTLs {
		//LoadX509KeyPair reads and parses a public/private key pair from a pair of files.
		//The files must contain PEM encoded data.
		//The certificate file may contain intermediate certificates following the leaf certificate to form a certificate chain.
		//On successful return, Certificate.Leaf will be nil because the parsed form of the certificate is not retained.
		cert, err := tls.LoadX509KeyPair(
			//certificate signed by intermediary for the client. It contains the public key.
			options.MTLsOptions.Certificate,
			//client key (needed to sign the requests. Only the public key can open the data)
			options.MTLsOptions.Pkey,
		)
		if err != nil {
			grpclog.Errorf("failed to read credentials: %s", err)
		}
		certPool := x509.NewCertPool()
		// chain is composed by default by ca.cert.pem and intermediate.cert.pem
		bs, err := ioutil.ReadFile(options.MTLsOptions.ClientCAs)
		if err != nil {
			grpclog.Errorf("failed to read ca cert: %s", err)
		}

		// AppendCertsFromPEM attempts to parse a series of PEM encoded certificates.
		// It appends any certificates found to s and reports whether any certificates were successfully parsed.
		// On many Linux systems, /etc/ssl/cert.pem will contain the system wide set of root CAs
		// in a format suitable for this function.
		ok := certPool.AppendCertsFromPEM(bs)
		if !ok {
			grpclog.Errorf("failed to append certs")
		}

		transportCreds := credentials.NewTLS(&tls.Config{
			// ServerName is used to verify the hostname on the returned
			// certificates unless InsecureSkipVerify is given. It is also included
			// in the client's handshake to support virtual hosting unless it is
			// an IP address.
			ServerName: "localhost",
			// Certificates contains one or more certificate chains to present to the
			// other side of the connection. The first certificate compatible with the
			// peer's requirements is selected automatically.
			// Server configurations must set one of Certificates, GetCertificate or
			// GetConfigForClient. Clients doing client-authentication may set either
			// Certificates or GetClientCertificate.
			Certificates: []tls.Certificate{cert},
			// Safe store, trusted certificate list
			// Server need to use one certificate presents in this lists.
			// RootCAs defines the set of root certificate authorities
			// that clients use when verifying server certificates.
			// If RootCAs is nil, TLS uses the host's root CA set.
			RootCAs: certPool,
		})
		opts = []grpc.DialOption{grpc.WithTransportCredentials(transportCreds)}
	}

	grpcServerEndpoint := flag.String("grpc-server-endpoint", options.Address+":"+strconv.Itoa(options.Port), "gRPC server endpoint")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := runtime.NewServeMux()

	handler := cors.Default().Handler(mux)

	conn, err := grpc.Dial(*grpcServerEndpoint, opts...)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", grpcServerEndpoint, cerr)
			}
			return
		}
		go func() {
			<-ctx.Done()
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", grpcServerEndpoint, cerr)
			}
		}()
	}()

	logger := logger.New("immugw", os.Stderr)
	client := schema.NewImmuServiceClient(conn)
	c := cache.NewFileCache()
	rs := rp.NewRootService(client, c)

	_, err = rs.GetRoot(ctx)
	if err != nil {
		return err
	}
	ssh := gw.NewSafesetHandler(mux, client, rs)
	sgh := gw.NewSafegetHandler(mux, client, rs)
	hh := gw.NewHistoryHandler(mux, client, rs)
	sr := gw.NewSafeReferenceHandler(mux, client, rs)
	sza := gw.NewSafeZAddHandler(mux, client, rs)
	mux.Handle(http.MethodPost, schema.Pattern_ImmuService_SafeSet_0(), ssh.Safeset)
	mux.Handle(http.MethodPost, schema.Pattern_ImmuService_SafeGet_0(), sgh.Safeget)
	mux.Handle(http.MethodGet, schema.Pattern_ImmuService_History_0(), hh.History)
	mux.Handle(http.MethodPost, schema.Pattern_ImmuService_SafeReference_0(), sr.SafeReference)
	mux.Handle(http.MethodPost, schema.Pattern_ImmuService_SafeZAdd_0(), sza.SafeZAdd)

	err = schema.RegisterImmuServiceHandlerFromEndpoint(ctx, mux, *grpcServerEndpoint, opts)
	if err != nil {
		return err
	}

	var protoReq empty.Empty
	var metadata runtime.ServerMetadata
	if healt, err := client.Health(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD)); err != nil {
		logger.Infof(err.Error())
		return err
	} else {
		if !healt.GetStatus() {
			msg := fmt.Sprintf("Immudb not in healt at %s:%d", immudhost, immudport)
			logger.Infof(msg)
			return errors.New(msg)
		} else {
			logger.Infof(fmt.Sprintf("Immudb is listening at %s:%d", immudhost, immudport))
		}
	}
	logger.Infof("Starting immugw at %s:%d", host, port)
	return http.ListenAndServe(host+":"+strconv.Itoa(port), handler)
}
