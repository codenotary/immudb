package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	rp "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/rs/cors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"net/http"
	"os"
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

	immugwCmd.Flags().StringP("port", "p", "8081", "immugw port number")
	immugwCmd.Flags().StringP("host", "s", "127.0.0.1", "immugw host address")
	immugwCmd.Flags().StringP("immudport", "j", "8080", "immudb port number")
	immugwCmd.Flags().StringP("immudhost", "y", "127.0.0.1", "immudb host address")

	if err := immugwCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serve(cmd *cobra.Command, args []string) error {
	port, err := cmd.Flags().GetString("port")
	if err != nil {
		return err
	}
	host, err := cmd.Flags().GetString("host")
	if err != nil {
		return err
	}
	immudport, err := cmd.Flags().GetString("immudport")
	if err != nil {
		return err
	}
	immudhost, err := cmd.Flags().GetString("immudhost")
	if err != nil {
		return err
	}
	grpcServerEndpoint := flag.String("grpc-server-endpoint", immudhost+":"+immudport, "gRPC server endpoint")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := runtime.NewServeMux()

	handler := cors.Default().Handler(mux)

	opts := []grpc.DialOption{grpc.WithInsecure()}

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
		if healt.GetStatus() != true {
			msg := fmt.Sprintf("Immudb not in healt at %s:%s", immudhost, immudport)
			logger.Infof(msg)
			return errors.New(msg)
		} else {
			logger.Infof(fmt.Sprintf("Immudb is listening at %s:%s", immudhost, immudport))
		}
	}
	logger.Infof("Starting immugw at %s:%s", host, port)
	return http.ListenAndServe(host+":"+port, handler)
}
