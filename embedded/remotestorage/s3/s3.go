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
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/remotestorage"
)

type Storage struct {
	endpoint    string
	accessKeyID string
	secretKey   string
	bucket      string
	prefix      string
	location    string
	httpClient  *http.Client
}

var (
	ErrInvalidArguments = errors.New("invalid arguments")
	ErrInvalidResponse  = errors.New("invalid response code")
	ErrTooManyRedirects = errors.New("too many redirects")
)

const maxRedirects = 5

func Open(
	endpoint string,
	accessKeyID string,
	secretKey string,
	bucket string,
	location string,
	prefix string,
) (remotestorage.Storage, error) {

	// Endpoint must always end with '/'
	endpoint = strings.TrimRight(endpoint, "/") + "/"

	// Bucket must have no '/' at all
	bucket = strings.Trim(bucket, "/")
	if strings.Contains(bucket, "/") {
		return nil, ErrInvalidArguments
	}

	// if prefix is not empty, it must end with '/'
	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix = prefix + "/"
	}

	return &Storage{
		endpoint:    endpoint,
		accessKeyID: accessKeyID,
		secretKey:   secretKey,
		bucket:      bucket,
		location:    location,
		prefix:      prefix,
		httpClient: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (s *Storage) String() string {
	url, err := s.originalRequestURL("")
	if err != nil {
		return "s3(misconfigured)"
	}
	return "s3:" + url
}

func (s *Storage) originalRequestURL(objectName string) (string, error) {
	reqURL, err := url.Parse(fmt.Sprintf("%s%s%s",
		s.endpoint,
		s.prefix,
		objectName,
	))
	if err != nil {
		return "", err
	}

	if !strings.HasPrefix(reqURL.Host, s.bucket+".") {
		reqURL.Path = "/" + s.bucket + reqURL.Path
	}

	return reqURL.String(), nil
}

func (s *Storage) s3SignedRequest(
	ctx context.Context,
	url string,
	method string,
	body io.Reader,
	contentType string,
	setupRequest func(req *http.Request) error,
	date time.Time,
) (
	*http.Request,
	error,
) {
	if s.location == "" {
		// Missing location configuration, try V2 signatures that don't require it
		return s.s3SignedRequestV2(ctx, url, method, body, contentType, setupRequest, date)
	}

	return s.s3SignedRequestV4(ctx, url, method, body, contentType, "", setupRequest, date)
}

func (s *Storage) s3SignedRequestV4(
	ctx context.Context,
	reqUrl string,
	method string,
	body io.Reader,
	contentType string,
	contentSha256 string,
	setupRequest func(req *http.Request) error,
	t time.Time,
) (
	*http.Request,
	error,
) {
	const authorization = "AWS4-HMAC-SHA256"
	const unsignedPayload = "UNSIGNED-PAYLOAD"
	const serviceName = "s3"

	req, err := http.NewRequestWithContext(ctx, method, reqUrl, body)
	if err != nil {
		return nil, err
	}
	err = setupRequest(req)
	if err != nil {
		return nil, err
	}

	timeISO8601 := t.Format("20060102T150405Z")
	timeYYYYMMDD := t.Format("20060102")
	scope := timeYYYYMMDD + "/" + s.location + "/" + serviceName + "/aws4_request"
	credential := s.accessKeyID + "/" + scope

	if contentSha256 == "" {
		contentSha256 = unsignedPayload
	}

	req.Header.Set("X-Amz-Date", timeISO8601)
	req.Header.Set("X-Amz-Content-Sha256", contentSha256)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	canonicalURI := req.URL.Path // TODO: This may require some encoding
	canonicalQueryString := req.URL.Query().Encode()

	signerHeadersList := []string{"host"}
	for h := range req.Header {
		signerHeadersList = append(signerHeadersList, strings.ToLower(h))
	}
	sort.Strings(signerHeadersList)
	signedHeaders := strings.Join(signerHeadersList, ";")
	canonicalHeaders := ""
	for _, h := range signerHeadersList {
		if h == "host" {
			canonicalHeaders = canonicalHeaders + h + ":" + req.Host + "\n"
		} else {
			canonicalHeaders = canonicalHeaders + h + ":" + req.Header.Get(h) + "\n"
		}
	}

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		contentSha256,
	}, "\n")
	canonicalRequestHash := sha256.Sum256([]byte(canonicalRequest))

	stringToSign := authorization + "\n" +
		timeISO8601 + "\n" +
		scope + "\n" +
		hex.EncodeToString(canonicalRequestHash[:])

	hmacSha256 := func(key []byte, data []byte) []byte {
		h := hmac.New(sha256.New, key)
		h.Write(data)
		return h.Sum(nil)
	}

	dateKey := hmacSha256([]byte("AWS4"+s.secretKey), []byte(timeYYYYMMDD))
	dateRegionKey := hmacSha256(dateKey, []byte(s.location))
	dateRegionServiceKey := hmacSha256(dateRegionKey, []byte(serviceName))
	signingKey := hmacSha256(dateRegionServiceKey, []byte("aws4_request"))

	signature := hex.EncodeToString(hmacSha256(signingKey, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s,SignedHeaders=%s,Signature=%s",
		authorization,
		credential,
		signedHeaders,
		signature,
	))

	return req, nil
}

func (s *Storage) s3SignedRequestV2(
	ctx context.Context,
	url string,
	method string,
	body io.Reader,
	contentType string,
	setupRequest func(req *http.Request) error,
	t time.Time,
) (
	*http.Request,
	error,
) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	err = setupRequest(req)
	if err != nil {
		return nil, err
	}

	date := t.Format(http.TimeFormat)
	req.Header.Set("Date", date)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	signedPath := req.URL.Path
	if strings.HasPrefix(req.Host, s.bucket+".") {
		// Bucket name is passed through the domain name,
		// the signature however does take this bucked into account
		signedPath = "/" + s.bucket + signedPath
	}

	mac := hmac.New(sha1.New, []byte(s.secretKey))
	fmt.Fprintf(mac, "%s\n\n%s\n%s\n%s", method, contentType, date, signedPath)
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	req.Header.Set(
		"Authorization",
		fmt.Sprintf("AWS %s:%s", s.accessKeyID, signature),
	)

	return req, nil
}

// Get opens a remote s3 resource
func (s *Storage) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	if offs < 0 || size == 0 {
		return nil, ErrInvalidArguments
	}
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return nil, ErrInvalidArguments
	}

	url, err := s.originalRequestURL(name)
	if err != nil {
		return nil, err
	}

	resp, err := s.requestWithRedirects(
		ctx,
		"GET",
		url,
		[]int{200, 206},
		func() (io.Reader, string, error) { return nil, "", nil },
		func(req *http.Request) error {
			log.Printf("S3 %s %s range: %d %d",
				req.Method,
				req.URL,
				offs, size,
			)
			if size < 0 {
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offs))
			} else {
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offs, offs+size-1))
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &metricsCountingReadCloser{
		r: resp.Body,
		c: metricsDownloadBytes,
	}, nil
}

func (s *Storage) requestWithRedirects(
	ctx context.Context,
	method string,
	reqURL string,
	validStatusCodes []int,
	prepareData func() (io.Reader, string, error),
	setupRequest func(req *http.Request) error,

) (*http.Response, error) {

	for i := 0; i < maxRedirects; i++ {

		data, contentType, err := prepareData()
		if err != nil {
			return nil, err
		}

		req, err := s.s3SignedRequest(
			ctx,
			reqURL,
			method,
			data,
			contentType,
			setupRequest,
			time.Now().UTC(),
		)
		if err != nil {
			return nil, err
		}

		log.Printf("S3 %s %s", req.Method, req.URL)
		resp, err := s.httpClient.Do(req)
		if err != nil {
			log.Printf("S3 %s %s failed: %v", req.Method, req.URL, err)
			return nil, err
		}

		for _, validStatus := range validStatusCodes {
			if resp.StatusCode == validStatus {
				log.Printf("S3 %s %s %s", req.Method, req.URL, resp.Status)
				return resp, nil
			}
		}
		resp.Body.Close()

		switch resp.StatusCode {
		case 303:
			locationURL, err := url.Parse(resp.Header.Get("Location"))
			if err != nil {
				log.Printf(
					"S3 %s %s failed: invalid `Location` header: '%s' when doing redirection",
					req.Method,
					req.URL,
					req.Header.Get("Location"),
				)
				return nil, ErrInvalidResponse
			}

			// Switch to simple GET request
			method = "GET"
			prepareData = func() (io.Reader, string, error) { return nil, "", nil }
			setupRequest = func(req *http.Request) error { return nil }

			reqURL = req.URL.ResolveReference(locationURL).String()
			log.Printf("S3 %s redirect to GET %s", req.Method, reqURL)

		case 301, 302, 307, 308:
			locationURL, err := url.Parse(resp.Header.Get("Location"))
			if err != nil {
				log.Printf(
					"S3 %s %s failed: invalid `Location` header: '%s' when doing redirection",
					req.Method,
					req.URL,
					req.Header.Get("Location"),
				)
				return nil, ErrInvalidResponse
			}

			reqURL = req.URL.ResolveReference(locationURL).String()
			log.Printf("S3 %s redirect to %s", req.Method, reqURL)

		default:
			log.Printf(
				"S3 %s %s failed with status code %d (%s)",
				req.Method,
				req.URL,
				resp.StatusCode,
				resp.Status,
			)
			return nil, ErrInvalidResponse
		}
	}
	log.Printf("S3 %s %s failed - too many redirects", method, reqURL)
	return nil, ErrTooManyRedirects
}

// Put writes a remote s3 resource
func (s *Storage) Put(ctx context.Context, name string, fileName string) error {
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return ErrInvalidArguments
	}

	// S3 is using 307 redirects that must preserve POST body,
	// this can not be handled by the http go module because it requires reopening the reader

	putURL, err := s.originalRequestURL(name)
	if err != nil {
		return err
	}

	fl, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer fl.Close()
	flStat, err := fl.Stat()
	if err != nil {
		return err
	}

	resp, err := s.requestWithRedirects(
		ctx,
		"PUT",
		putURL,
		[]int{200},
		func() (io.Reader, string, error) {
			_, err := fl.Seek(0, io.SeekStart)
			if err != nil {
				return nil, "", err
			}
			return &metricsCountingReadCloser{
					r: ioutil.NopCloser(fl),
					c: metricsUploadBytes,
				},
				"application/octet-stream",
				nil
		},
		func(req *http.Request) error {
			req.ContentLength = flStat.Size()
			return nil
		},
	)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Exists checks if a remove resource exists and can be read.
// Note that due to an asynchronous nature of cloud storage,
// a resource stored with the Put method may not be immediately accessible.
func (s *Storage) Exists(ctx context.Context, name string) (bool, error) {
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return false, ErrInvalidArguments
	}

	url, err := s.originalRequestURL(name)
	if err != nil {
		return false, err
	}

	resp, err := s.requestWithRedirects(
		ctx, "HEAD", url,
		[]int{200, 404},
		func() (io.Reader, string, error) { return nil, "", nil },
		func(req *http.Request) error { return nil },
	)
	if err != nil {
		return false, err
	}
	resp.Body.Close()

	return resp.StatusCode == 200, nil
}

func (s *Storage) ListEntries(ctx context.Context, path string) ([]remotestorage.EntryInfo, []string, error) {

	if path != "" {
		if !strings.HasSuffix(path, "/") ||
			strings.Contains(path, "//") {
			return nil, nil, ErrInvalidArguments
		}
	}

	prefix := s.prefix + path

	baseUrl, err := s.originalRequestURL("")
	if err != nil {
		return nil, nil, err
	}

	// Path for the list operation is passed through query parameters
	baseUrl = strings.TrimSuffix(baseUrl, s.prefix)

	urlValues := url.Values{}
	urlValues.Set("list-type", "2")
	urlValues.Set("encoding-type", "url")
	urlValues.Set("delimiter", "/")
	urlValues.Set("prefix", prefix)

	entries := []remotestorage.EntryInfo{}
	subPaths := []string{}

	for i := 1; ; i++ {
		resp, err := s.requestWithRedirects(
			ctx, "GET", baseUrl+"?"+urlValues.Encode(),
			[]int{200},
			func() (io.Reader, string, error) { return nil, "", nil },
			func(req *http.Request) error { return nil },
		)
		if err != nil {
			return nil, nil, err
		}
		defer resp.Body.Close()

		respParsed := struct {
			Contents []struct {
				Key  string
				Size int64
			}
			CommonPrefixes        []struct{ Prefix string }
			IsTruncated           bool
			NextContinuationToken string
		}{}

		err = xml.NewDecoder(resp.Body).Decode(&respParsed)
		if err != nil {
			return nil, nil, err
		}

		for _, object := range respParsed.Contents {
			entries = append(entries, remotestorage.EntryInfo{
				Name: strings.TrimPrefix(object.Key, prefix),
				Size: object.Size,
			})
		}
		for _, subPath := range respParsed.CommonPrefixes {
			if !strings.HasPrefix(subPath.Prefix, prefix) || !strings.HasSuffix(subPath.Prefix, "/") {
				return nil, nil, ErrInvalidResponse
			}

			p := subPath.Prefix[len(prefix) : len(subPath.Prefix)-1]
			if p == "." || p == ".." || strings.ContainsAny(p, "\\/:") {
				// Avoid exploitation by a malicious server
				return nil, nil, ErrInvalidResponse
			}

			subPaths = append(subPaths, p)
		}

		if !respParsed.IsTruncated {
			break
		}

		urlValues.Set("continuation-token", respParsed.NextContinuationToken)
	}

	if !sort.SliceIsSorted(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name }) ||
		!sort.StringsAreSorted(subPaths) {
		return nil, nil, ErrInvalidResponse
	}

	return entries, subPaths, nil
}

var _ remotestorage.Storage = (*Storage)(nil)
