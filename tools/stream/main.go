package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/stream"
	"google.golang.org/grpc/metadata"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
)

const FileVideoName = "/home/falce/Video/STS-127_Launch_HD_orig.mp4"

type handler struct {
	cli client.ImmuClient
	ctx context.Context
}

func (h *handler) stream(w http.ResponseWriter, r *http.Request) {

	rangeValue := r.Header.Get("range")
	fmt.Println("Range:")
	fmt.Println(rangeValue)

	buf := bytes.NewBuffer(nil)
	f, err := os.Open(FileVideoName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = io.Copy(buf, f)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fileSize := strconv.Itoa(int(fi.Size()))

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Content-Length", fileSize)
	w.Header().Set("Last-Modified", fi.ModTime().String())

	w.WriteHeader(206)
	_, err = w.Write(buf.Bytes())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *handler) upload(w http.ResponseWriter, r *http.Request) {

	s, err := h.cli.SetStream(h.ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	kvs := stream.NewKvStreamSender(stream.NewMsgSender(s))

	filename := FileVideoName
	f, err := os.Open(filename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	kv := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(filename))),
			Size:    len(filename),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(f),
			Size:    int(fi.Size()),
		},
	}

	err = kvs.Send(kv)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func main() {

	cli, err := getImmuClient()
	if err != nil {
		log.Fatal(err)
	}

	ctx, err := getAuthContext(cli)
	if err != nil {
		log.Fatal(err)
	}

	h := &handler{
		cli: cli,
		ctx: ctx,
	}

	http.HandleFunc("/upload", h.upload)
	http.HandleFunc("/stream", h.stream)

	err = http.ListenAndServe(":8085", nil)
	if err != nil {
		log.Fatal(err)
	}
}

func getImmuClient() (client.ImmuClient, error) {
	cli, err := client.NewImmuClient(client.DefaultOptions())
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func getAuthContext(cli client.ImmuClient) (context.Context, error) {
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		return nil, err
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{Databasename: "defaultdb"})
	if err != nil {
		return nil, err
	}

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	return ctx, nil
}
