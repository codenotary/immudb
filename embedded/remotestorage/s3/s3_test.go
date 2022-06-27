/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package s3

import (
	"context"
	"fmt"
	"io/fs"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	s, err := Open(
		"http://localhost:9000",
		"minioadmin",
		"minioadmin",
		"immudb",
		"",
		"prefix",
	)
	require.NoError(t, err)
	require.NotNil(t, s)
	require.Equal(t, "s3:http://localhost:9000/immudb/prefix/", s.String())
}

func TestCornerCases(t *testing.T) {
	t.Run("bucket name can not be empty", func(t *testing.T) {
		s, err := Open(
			"http://localhost:9000",
			"minioadmin",
			"minioadmin",
			"",
			"",
			"",
		)
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsBucketEmpty)
		require.Nil(t, s)
	})

	t.Run("bucket name can not contain /", func(t *testing.T) {
		s, err := Open(
			"http://localhost:9000",
			"minioadmin",
			"minioadmin",
			"immudb/test",
			"",
			"",
		)
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsBucketSlash)
		require.Nil(t, s)
	})

	t.Run("prefix must be correctly normalized", func(t *testing.T) {
		s, err := Open(
			"http://localhost:9000",
			"minioadmin",
			"minioadmin",
			"immudb",
			"",
			"",
		)
		require.NoError(t, err)
		require.Equal(t, "", s.(*Storage).prefix)

		s, err = Open(
			"http://localhost:9000",
			"minioadmin",
			"minioadmin",
			"immudb",
			"",
			"/test/",
		)
		require.NoError(t, err)
		require.Equal(t, "test/", s.(*Storage).prefix)

		s, err = Open(
			"http://localhost:9000",
			"minioadmin",
			"minioadmin",
			"immudb",
			"",
			"/test",
		)
		require.NoError(t, err)
		require.Equal(t, "test/", s.(*Storage).prefix)
	})

	t.Run("invalid url", func(t *testing.T) {
		s, err := Open(
			"h**s://localhost:9000",
			"minioadmin",
			"minioadmin",
			"bucket",
			"",
			"",
		)
		require.NoError(t, err)
		require.Equal(t, "s3(misconfigured)", s.String())
	})

	t.Run("invalid get / put path", func(t *testing.T) {
		s, err := Open(
			"htts://localhost:9000",
			"minioadmin",
			"minioadmin",
			"bucket",
			"",
			"",
		)
		require.NoError(t, err)

		_, err = s.Get(context.Background(), "/file", 0, -1)
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsNameSlash)

		_, err = s.Get(context.Background(), "file/", 0, -1)
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsNameSlash)

		err = s.Put(context.Background(), "/file", "/tmp/test.txt")
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsNameSlash)

		err = s.Put(context.Background(), "file/", "/tmp/test.txt")
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsNameSlash)
	})

	t.Run("invalid get offset / size", func(t *testing.T) {
		s, err := Open(
			"htts://localhost:9000",
			"minioadmin",
			"minioadmin",
			"bucket",
			"",
			"",
		)
		require.NoError(t, err)

		_, err = s.Get(context.Background(), "file", 0, 0)
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsOffsSize)
	})

	t.Run("invalid list path", func(t *testing.T) {
		s, err := Open(
			"https://localhost:9000",
			"minioadmin",
			"minioadmin",
			"bucket",
			"",
			"",
		)
		require.NoError(t, err)

		_, _, err = s.ListEntries(context.Background(), "prefix-no-slash")
		require.ErrorIs(t, err, ErrInvalidArguments)

		_, _, err = s.ListEntries(context.Background(), "prefix-double-slash//")
		require.ErrorIs(t, err, ErrInvalidArguments)
	})

	t.Run("invalid http status code from the server", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "test error", http.StatusInternalServerError)
		}))
		defer ts.Close()

		s, err := Open(ts.URL, "", "", "bucket", "", "")
		require.NoError(t, err)

		ctx := context.Background()

		_, err = s.Get(ctx, "object1", 0, -1)
		require.ErrorIs(t, err, ErrInvalidResponse)
	})

	t.Run("invalid upload file path", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Fail(t, "Should not call the server")
		}))
		defer ts.Close()

		s, err := Open(ts.URL, "", "", "bucket", "", "")
		require.NoError(t, err)

		ctx := context.Background()

		err = s.Put(ctx, "object1", "/invalid/file/path/that/does/not/exist")
		require.IsType(t, &fs.PathError{}, err)
	})
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

func TestHandlingRedirects(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		switch r.URL.Path {
		case "/bucket/object1":
			require.Equal(t, "GET", r.Method)
			http.Redirect(w, r, "/bucket/object2", http.StatusSeeOther)

		case "/bucket/object2":
			require.Equal(t, "GET", r.Method)
			http.Redirect(w, r, "/bucket/object3", http.StatusPermanentRedirect)

		case "/bucket/object3":
			require.Equal(t, "GET", r.Method)

			_, err := w.Write([]byte("Hello world"))
			require.NoError(t, err)

		case "/bucket/object4":
			require.Equal(t, "GET", r.Method)
			http.Redirect(w, r, "/bucket/object4", http.StatusTemporaryRedirect)

		case "/bucket/object5":
			require.Equal(t, "GET", r.Method)
			http.Redirect(w, r, "h**p://invalid", http.StatusSeeOther)

		case "/bucket/object6":
			require.Equal(t, "GET", r.Method)
			http.Redirect(w, r, "h**p://invalid", http.StatusTemporaryRedirect)

		case "/bucket/object7":
			require.Equal(t, "PUT", r.Method)
			http.Redirect(w, r, "h**p://invalid", http.StatusSeeOther)

		case "/bucket/object8":
			require.Equal(t, "PUT", r.Method)
			http.Redirect(w, r, "h**p://invalid", http.StatusTemporaryRedirect)

		default:
			require.Fail(t, "Unknown request")
		}

	}))
	defer ts.Close()

	s, err := Open(ts.URL, "", "", "bucket", "", "")
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("open bucket with redirects", func(t *testing.T) {

		io, err := s.Get(ctx, "object1", 0, -1)
		require.NoError(t, err)

		b, err := ioutil.ReadAll(io)
		require.NoError(t, err)

		err = io.Close()
		require.NoError(t, err)

		require.Equal(t, []byte("Hello world"), b)
	})

	t.Run("detect infinite redirect loop", func(t *testing.T) {
		_, err := s.Get(ctx, "object4", 0, -1)
		require.ErrorIs(t, err, ErrTooManyRedirects)
	})

	t.Run("error getting 303 redirect", func(t *testing.T) {
		_, err := s.Get(ctx, "object5", 0, -1)
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.Contains(t, err.Error(), "failed to parse Location header")
	})

	t.Run("error getting 307 redirect", func(t *testing.T) {
		_, err := s.Get(ctx, "object6", 0, -1)
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.Contains(t, err.Error(), "failed to parse Location header")
	})

	t.Run("error getting 303 redirect while PUT request", func(t *testing.T) {
		fl, err := ioutil.TempFile("", "")
		require.NoError(t, err)
		fmt.Fprintf(fl, "Hello world")
		fl.Close()
		defer os.Remove(fl.Name())

		err = s.Put(ctx, "object7", fl.Name())
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.Contains(t, err.Error(), "failed to parse Location header")
	})

	t.Run("error getting 307 redirect", func(t *testing.T) {
		fl, err := ioutil.TempFile("", "")
		require.NoError(t, err)
		fmt.Fprintf(fl, "Hello world")
		fl.Close()
		defer os.Remove(fl.Name())

		err = s.Put(ctx, "object8", fl.Name())
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.Contains(t, err.Error(), "failed to parse Location header")
	})
}

func TestListEntries(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/bucket/", r.URL.Path)

		switch r.URL.Query().Get("prefix") {
		case "path1/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>ExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path1/photos1/</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path1/photos2/</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path2/":
			w.Write([]byte(`
				< this is not a valid xml >
			`))

		case "path3/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>ExampleObject2.txt</Key>
					</Contents>
					<Contents>
						<Key>ExampleObject1.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path3/photos1/</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path3/photos2/</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path4/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>ExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path4/photos2/</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path4/photos1/</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path5/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>ExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>invalid-prefix/photos1/</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>invalid-prefix/photos2/</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path6/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>ExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path6/photos1</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path6/photos2</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path7/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>ExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path7/photos1/</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path7/../photos2/</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path8/":
			switch r.URL.Query().Get("continuation-token") {
			case "":
				w.Write([]byte(`
					<?xml version="1.0" encoding="UTF-8"?>
					<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<IsTruncated>true</IsTruncated>
						<NextContinuationToken>cont-token</NextContinuationToken>
						<Contents>
							<Key>ExampleObject1.txt</Key>
						</Contents>
						<Contents>
							<Key>ExampleObject2.txt</Key>
						</Contents>
						<CommonPrefixes>
							<Prefix>path8/photos1/</Prefix>
						</CommonPrefixes>
						<CommonPrefixes>
							<Prefix>path8/photos2/</Prefix>
						</CommonPrefixes>
					</ListBucketResult>
				`))
			case "cont-token":
				w.Write([]byte(`
					<?xml version="1.0" encoding="UTF-8"?>
					<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
						<IsTruncated>false</IsTruncated>
						<Contents>
							<Key>ExampleObject3.txt</Key>
						</Contents>
						<Contents>
							<Key>ExampleObject4.txt</Key>
						</Contents>
						<CommonPrefixes>
							<Prefix>path8/photos3/</Prefix>
						</CommonPrefixes>
						<CommonPrefixes>
							<Prefix>path8/photos4/</Prefix>
						</CommonPrefixes>
					</ListBucketResult>
				`))
			default:
				require.Fail(t, "invalid continuation token")
			}

		default:
			require.Fail(t, "Invalid request")
		}

	}))
	defer ts.Close()

	s, err := Open(ts.URL, "", "", "bucket", "", "")
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("correct response", func(t *testing.T) {
		entries, subPaths, err := s.ListEntries(ctx, "path1/")
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.Len(t, subPaths, 2)
	})

	t.Run("invalid xml response when listing", func(t *testing.T) {
		_, _, err := s.ListEntries(ctx, "path2/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseXmlDecodeError)
	})

	t.Run("entries not sorted", func(t *testing.T) {
		_, _, err := s.ListEntries(ctx, "path3/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseEntriesNotSorted)
	})

	t.Run("sub paths not sorted", func(t *testing.T) {
		_, _, err := s.ListEntries(ctx, "path4/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseSubPathsNotSorted)
	})

	t.Run("sub paths incorrectly prefixed", func(t *testing.T) {
		_, _, err := s.ListEntries(ctx, "path5/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseSubPathsWrongPrefix)
	})

	t.Run("sub paths incorrectly prefixed", func(t *testing.T) {
		_, _, err := s.ListEntries(ctx, "path6/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseSubPathsWrongSuffix)
	})

	t.Run("sub paths contain incorrect characters", func(t *testing.T) {
		_, _, err := s.ListEntries(ctx, "path7/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseSubPathMalicious)
	})

	t.Run("query with continuation token", func(t *testing.T) {
		entries, subPaths, err := s.ListEntries(ctx, "path8/")
		require.NoError(t, err)
		require.Len(t, entries, 4)
		require.Len(t, subPaths, 4)
	})

}
