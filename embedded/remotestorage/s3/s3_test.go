// +build minio

package s3

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleUpload(t *testing.T) {

	s, err := Open(
		"http://localhost:9000",
		"minioadmin",
		"minioadmin",
		"immudb",
		"",
	)
	require.NoError(t, err)

	ctx := context.Background()

	// Reader is wrapped to ensure it's not recognized as the in-memory buffer.
	// Standard http lib in golang detectx Content-Length headers for bytes.Buffer readers.
	fl, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	fmt.Fprintf(fl, "Hello world")
	fl.Close()

	err = s.Put(ctx, "test1", fl.Name())
	require.NoError(t, err)
}
