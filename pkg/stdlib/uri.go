package stdlib

import (
	"crypto/tls"
	"errors"
	"github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net/url"
	"strconv"
	"strings"
)

func ParseConfig(uri string) (*client.Options, error) {
	if uri != "" && strings.HasPrefix(uri, "immudb://") {
		url, err := url.Parse(uri)
		if err != nil {
			return nil, ErrBadQueryString
		}
		pw, _ := url.User.Password()
		port, _ := strconv.Atoi(url.Port())

		sslMode := url.Query().Get("sslmode")
		dialOptions, err := dialOptions(sslMode)
		if err != nil {
			return nil, err
		}

		cliOpts := client.DefaultOptions().
			WithUsername(url.User.Username()).
			WithPassword(pw).
			WithPort(port).
			WithAddress(url.Hostname()).
			WithDatabase(url.Path[1:]).
			WithDialOptions(dialOptions)
		return cliOpts, nil
	}
	return nil, ErrBadQueryString
}

func GetUri(o *client.Options) string {
	u := url.URL{
		Scheme: "immudb",
		User: url.UserPassword(
			o.Username,
			o.Password,
		),
		Host: strings.Join([]string{o.Address, ":", strconv.Itoa(o.Port)}, ""),
		Path: o.Database,
	}
	return u.String()
}

func dialOptions(sslmode string) ([]grpc.DialOption, error) {
	if sslmode == "" {
		sslmode = "prefer"
	}
	switch sslmode {
	case "disable":
		return []grpc.DialOption{grpc.WithInsecure()}, nil
	case "allow", "prefer":
		return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}))}, nil
	case "require":
		return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{}))}, nil
	case "verify-ca":
		return nil, ErrNotImplemented
	case "verify-full":
		return nil, ErrNotImplemented
	default:
		return nil, errors.New("sslmode is invalid")
	}
}
