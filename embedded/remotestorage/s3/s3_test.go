/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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
		false,
		"",
		"minioadmin",
		"minioadmin",
		"immudb",
		"",
		"prefix",
		"",
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, s)
	require.Equal(t, "s3", s.Kind())
	require.Equal(t, "s3:http://localhost:9000/immudb/prefix/", s.String())
}

func TestValidateName(t *testing.T) {
	for _, d := range []struct {
		name     string
		isFolder bool
		err      error
	}{
		{"", false, nil},
		{"", true, nil},
		{"test", false, nil},
		{"test/", true, nil},
		{"test/name", false, nil},
		{"test/name/", true, nil},
		{"/test", false, ErrInvalidArgumentsNameStartSlash},
		{"/test", true, ErrInvalidArgumentsNameStartSlash},
		{"test/", false, ErrInvalidArgumentsNameEndSlash},
		{"test", true, ErrInvalidArgumentsPathNoEndSlash},
		{"test//name", false, ErrInvalidArgumentsInvalidName},
		{"test/./name", false, ErrInvalidArgumentsInvalidName},
		{"test/../test", false, ErrInvalidArgumentsInvalidName},
		{"./test", false, ErrInvalidArgumentsInvalidName},
		{"../test", false, ErrInvalidArgumentsInvalidName},
	} {
		t.Run(fmt.Sprintf("%+v", d), func(t *testing.T) {
			s := Storage{}
			err := s.validateName(d.name, d.isFolder)
			require.ErrorIs(t, err, d.err)
		})
	}
}

func TestCornerCases(t *testing.T) {
	t.Run("bucket name can not be empty", func(t *testing.T) {
		s, err := Open(
			"http://localhost:9000",
			false,
			"",
			"minioadmin",
			"minioadmin",
			"",
			"",
			"",
			"",
			false,
		)
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsBucketEmpty)
		require.Nil(t, s)
	})

	t.Run("bucket name can not contain /", func(t *testing.T) {
		s, err := Open(
			"http://localhost:9000",
			false,
			"",
			"minioadmin",
			"minioadmin",
			"immudb/test",
			"",
			"",
			"",
			false,
		)
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsBucketSlash)
		require.Nil(t, s)
	})

	t.Run("prefix must be correctly normalized", func(t *testing.T) {
		s, err := Open(
			"http://localhost:9000",
			false,
			"",
			"minioadmin",
			"minioadmin",
			"immudb",
			"",
			"",
			"",
			false,
		)
		require.NoError(t, err)
		require.Equal(t, "", s.(*Storage).prefix)

		s, err = Open(
			"http://localhost:9000",
			false,
			"",
			"minioadmin",
			"minioadmin",
			"immudb",
			"",
			"/test/",
			"",
			false,
		)
		require.NoError(t, err)
		require.Equal(t, "test/", s.(*Storage).prefix)

		s, err = Open(
			"http://localhost:9000",
			false,
			"",
			"minioadmin",
			"minioadmin",
			"immudb",
			"",
			"/test",
			"",
			false,
		)
		require.NoError(t, err)
		require.Equal(t, "test/", s.(*Storage).prefix)
	})

	t.Run("invalid url", func(t *testing.T) {
		s, err := Open(
			"h**s://localhost:9000",
			false,
			"",
			"minioadmin",
			"minioadmin",
			"bucket",
			"",
			"",
			"",
			false,
		)
		require.NoError(t, err)
		require.Equal(t, "s3(misconfigured)", s.String())
	})

	t.Run("invalid get / put / exists path", func(t *testing.T) {
		s, err := Open(
			"htts://localhost:9000",
			false,
			"",
			"minioadmin",
			"minioadmin",
			"bucket",
			"",
			"",
			"",
			false,
		)
		require.NoError(t, err)

		_, err = s.Get(context.Background(), "/file", 0, -1)
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsNameStartSlash)

		err = s.Put(context.Background(), "/file", "/tmp/test.txt")
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsNameStartSlash)

		_, err = s.Exists(context.Background(), "/file")
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsNameStartSlash)
	})

	t.Run("invalid get offset / size", func(t *testing.T) {
		s, err := Open(
			"htts://localhost:9000",
			false,
			"",
			"minioadmin",
			"minioadmin",
			"bucket",
			"",
			"",
			"",
			false,
		)
		require.NoError(t, err)

		_, err = s.Get(context.Background(), "file", 0, 0)
		require.ErrorIs(t, err, ErrInvalidArguments)
		require.ErrorIs(t, err, ErrInvalidArgumentsOffsSize)
	})

	t.Run("invalid role and credentials settings", func(t *testing.T) {
		s, err := Open(
			"htts://localhost:9000",
			true,
			"role",
			"minioadmin",
			"minioadmin",
			"bucket",
			"",
			"",
			"",
			false,
		)
		require.Error(t, err)
		require.Nil(t, s)
	})

	t.Run("invalid list path", func(t *testing.T) {
		s, err := Open(
			"https://localhost:9000",
			false,
			"",
			"minioadmin",
			"minioadmin",
			"bucket",
			"",
			"",
			"",
			false,
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

		s, err := Open(ts.URL, false, "", "", "", "bucket", "", "", "", false)
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

		s, err := Open(ts.URL, false, "", "", "", "bucket", "", "", "", false)
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
		false,
		"",
		"AKIAIOSFODNN7EXAMPLE",
		"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"examplebucket",
		"us-east-1",
		"",
		"",
		false,
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

	s, err := Open(ts.URL, false, "", "", "", "bucket", "", "", "", false)
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
						<Key>path1/ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>path1/ExampleObject2.txt</Key>
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
						<Key>path3/ExampleObject2.txt</Key>
					</Contents>
					<Contents>
						<Key>path3/ExampleObject1.txt</Key>
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
						<Key>path4/ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>path4/ExampleObject2.txt</Key>
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
						<Key>path5/ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>path5/ExampleObject2.txt</Key>
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
						<Key>path6/ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>path6/ExampleObject2.txt</Key>
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
						<Key>path7/ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>path7/ExampleObject2.txt</Key>
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
							<Key>path8/ExampleObject1.txt</Key>
						</Contents>
						<Contents>
							<Key>path8/ExampleObject2.txt</Key>
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
							<Key>path8/ExampleObject3.txt</Key>
						</Contents>
						<Contents>
							<Key>path8/ExampleObject4.txt</Key>
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

		case "path9/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>path9/ExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path9/photos1/</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path9/photos2/</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path10/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>path10/sub/ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>path10/ExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path10/photos1/</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path10/photos2/</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path11/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>path11/ExampleObject1.txt%</Key>
					</Contents>
					<Contents>
						<Key>path11/ExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path11/photos1/</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path11/photos2/</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path12/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>path12/ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>path12/ExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path12/photos1/%</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path12/photos2/</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path13/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>path13%2FExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>path13%2FExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path13%2Fphotos1%2F</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path13%2Fphotos2%2F</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path14/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>path14//ExampleObject1.txt</Key>
					</Contents>
					<Contents>
						<Key>path14/ExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path14/photos1/</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path14/photos2/</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		case "path15/":
			w.Write([]byte(`
				<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>path15/Example/Object1.txt</Key>
					</Contents>
					<Contents>
						<Key>path15/ExampleObject2.txt</Key>
					</Contents>
					<CommonPrefixes>
						<Prefix>path14/photos1/</Prefix>
					</CommonPrefixes>
					<CommonPrefixes>
						<Prefix>path14/photos2/</Prefix>
					</CommonPrefixes>
				</ListBucketResult>
			`))

		default:
			require.Fail(t, "Invalid request")
		}

	}))
	defer ts.Close()

	s, err := Open(ts.URL, false, "", "", "", "bucket", "", "", "", false)
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

	t.Run("entry name does not start with prefix", func(t *testing.T) {
		_, _, err := s.ListEntries(ctx, "path9/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseEntryNameWrongPrefix)
	})

	t.Run("entry name contains / character", func(t *testing.T) {
		_, _, err := s.ListEntries(ctx, "path10/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseEntryNameMalicious)
	})

	t.Run("entry name not correctly url encoded", func(t *testing.T) {
		_, _, err := s.ListEntries(ctx, "path11/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseEntryNameUnescape)
	})

	t.Run("subpath not correctly url encoded", func(t *testing.T) {
		_, _, err := s.ListEntries(ctx, "path12/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseSubPathUnescape)
	})

	t.Run("correctly handle url encoded entry names", func(t *testing.T) {
		entries, subPaths, err := s.ListEntries(ctx, "path13/")
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.Len(t, subPaths, 2)
		require.Equal(t, "ExampleObject1.txt", entries[0].Name)
		require.Equal(t, []string{"photos1", "photos2"}, subPaths)
	})

	t.Run("detect malicious object names", func(t *testing.T) {
		_, _, err := s.ListEntries(ctx, "path14/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseEntryNameMalicious)

		_, _, err = s.ListEntries(ctx, "path15/")
		require.ErrorIs(t, err, ErrInvalidResponse)
		require.ErrorIs(t, err, ErrInvalidResponseEntryNameMalicious)
	})
}
