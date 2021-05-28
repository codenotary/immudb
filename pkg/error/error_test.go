package error

import (
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestError(t *testing.T) {

	/*origError := errors.New("my first error")
	wrappedError := errors.Wrap(origError, "my wrapped error")

	fmt.Printf("%+v", wrappedError)


	st, ok := status.FromError(wrappedError)
	if ok {
		fmt.Printf("%+v", st.Err())
	}*/

	/*origStError := status.Error(codes.InvalidArgument, "my first status error")
	wrappedStError := errors.Wrap(origStError, "my wrapped status error")
	fmt.Printf("%+v", wrappedStError)

	wrappedWrappedStError := errors.Wrap(wrappedStError, "my wrapped wrapped status error")

	st, ok := status.FromError(wrappedWrappedStError)
	if ok {
		fmt.Printf("%+v", st.Err())
	}

	wrappedError := errors.Cause(wrappedWrappedStError)
	st, ok = status.FromError(wrappedError)
	if ok {
		fmt.Printf("%+v", st.Err())
		st, ok = status.FromError(st.Err())
		if ok {
			fmt.Printf("%+v", st.Err())
		}
	}*/

	firstError := errors.New("error cause")
	firstErrorWrapped := errors.Wrap(firstError, "my first error wrapped")
	firstErrorWrappedWrapped := errors.Wrap(firstErrorWrapped, "my first error wrapped wrapped")

	err2 := errors.Cause(firstErrorWrappedWrapped)
	fmt.Printf(err2.Error() + "\n")
	err1 := errors.Cause(err2)
	fmt.Printf(err1.Error() + "\n")
}

func TestGRPCError(t *testing.T) {
	serverOptions := server.DefaultOptions().WithMetricsServer(false).WithAdminPassword(auth.SysAdminPassword)
	s := server.DefaultServer().WithOptions(serverOptions).(*server.ImmuServer)
	defer os.RemoveAll(serverOptions.Dir)

	s.Initialize()

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte("wrong pw"),
	}
	_, err := s.Login(context.Background(), r)

	require.Error(t, err)
}
