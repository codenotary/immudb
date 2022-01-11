// +build minio

package s3

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimpleUpload(t *testing.T) {

	s, err := Open(
		"http://localhost:9000",
		"minioadmin",
		"minioadmin",
		"immudb",
		"",
		"",
	)
	require.NoError(t, err)

	ctx := context.Background()

	// Reader is wrapped to ensure it's not recognized as the in-memory buffer.
	// Standard http lib in golang detect Content-Length headers for bytes.Buffer readers.
	fl, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	fmt.Fprintf(fl, "Hello world")
	fl.Close()

	err = s.Put(ctx, "test1", fl.Name())
	require.NoError(t, err)
}

func TestSignatureV4(t *testing.T) {
	// Example request available at:
	//  https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
	s, err := Open(
		"https://examplebucket.s3.amazonaws.com",
		"AKIAIOSFODNN7EXAMPLE",
		"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"examplebucket",
		"us-east-1",
		"",
	)
	require.NoError(t, err)

	st := s.(*Storage)

	url, err := st.originalRequestURL("test.txt")
	require.NoError(t, err)

	req, err := st.s3SignedRequestV4(
		context.Background(),
		url,
		"GET",
		nil,
		"",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		func(req *http.Request) error {
			req.Header.Add("Range", "bytes=0-9")
			return nil
		},
		time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)
	require.NotNil(t, req)

	require.Equal(t,
		"AWS4-HMAC-SHA256 "+
			"Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"+
			"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,"+
			"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
		req.Header.Get("Authorization"),
	)
}
