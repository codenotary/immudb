/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/remotestorage"
)

type Storage struct {
	endpoint      string
	S3RoleEnabled bool
	s3Role        string
	accessKeyID   string
	secretKey     string
	bucket        string
	prefix        string
	location      string
	httpClient    *http.Client

	awsInstanceMetadataURL string
	awsCredsRefreshPeriod  time.Duration
}

var (
	ErrInvalidArguments               = errors.New("invalid arguments")
	ErrKeyCredentialsProvided         = errors.New("remote storage configuration already includes access key and/or secret key")
	ErrCredentialsCannotBeFound       = errors.New("cannot find credentials based on instance role remote storage")
	ErrInvalidArgumentsOffsSize       = fmt.Errorf("%w: negative offset or zero size", ErrInvalidArguments)
	ErrInvalidArgumentsNameStartSlash = fmt.Errorf("%w: name can not start with /", ErrInvalidArguments)
	ErrInvalidArgumentsNameEndSlash   = fmt.Errorf("%w: name can not end with /", ErrInvalidArguments)
	ErrInvalidArgumentsInvalidName    = fmt.Errorf("%w: invalid name", ErrInvalidArguments)
	ErrInvalidArgumentsPathNoEndSlash = fmt.Errorf("%w: path must end with /", ErrInvalidArguments)
	ErrInvalidArgumentsBucketSlash    = fmt.Errorf("%w: bucket name can not contain / character", ErrInvalidArguments)
	ErrInvalidArgumentsBucketEmpty    = fmt.Errorf("%w: bucket name can not be empty", ErrInvalidArguments)

	ErrInvalidResponse                     = errors.New("invalid response code")
	ErrInvalidResponseXmlDecodeError       = fmt.Errorf("%w: xml decode error", ErrInvalidResponse)
	ErrInvalidResponseEntriesNotSorted     = fmt.Errorf("%w: entries are not sorted", ErrInvalidResponse)
	ErrInvalidResponseEntryNameWrongPrefix = fmt.Errorf("%w: entry do not have correct prefix", ErrInvalidResponse)
	ErrInvalidResponseEntryNameMalicious   = fmt.Errorf("%w: entry name contains invalid characters", ErrInvalidResponse)
	ErrInvalidResponseEntryNameUnescape    = fmt.Errorf("%w: error un-escaping object name", ErrInvalidResponse)
	ErrInvalidResponseSubPathsNotSorted    = fmt.Errorf("%w: sub-paths are not sorted", ErrInvalidResponse)
	ErrInvalidResponseSubPathsWrongPrefix  = fmt.Errorf("%w: sub-paths do not have correct prefix", ErrInvalidResponse)
	ErrInvalidResponseSubPathsWrongSuffix  = fmt.Errorf("%w: sub-paths do end with '/' suffix", ErrInvalidResponse)
	ErrInvalidResponseSubPathMalicious     = fmt.Errorf("%w: sub-paths contain invalid characters", ErrInvalidResponse)
	ErrInvalidResponseSubPathUnescape      = fmt.Errorf("%w: error un-escaping object name", ErrInvalidResponse)

	ErrTooManyRedirects = errors.New("too many redirects")

	arnRoleRegex = regexp.MustCompile(`arn:.*\/(.*)`)
)

const maxRedirects = 5

func Open(
	endpoint string,
	S3RoleEnabled bool,
	s3Role string,
	accessKeyID string,
	secretKey string,
	bucket string,
	location string,
	prefix string,
	awsInstanceMetadataURL string,
) (remotestorage.Storage, error) {

	// Endpoint must always end with '/'
	endpoint = strings.TrimRight(endpoint, "/") + "/"

	// Bucket must have no '/' at all
	bucket = strings.Trim(bucket, "/")
	if strings.Contains(bucket, "/") {
		return nil, ErrInvalidArgumentsBucketSlash
	}

	// Bucket name must not be empty
	if bucket == "" {
		return nil, ErrInvalidArgumentsBucketEmpty
	}

	// if prefix is not empty, it must end with '/'
	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix = prefix + "/"
	}

	s3storage := &Storage{
		endpoint:      endpoint,
		S3RoleEnabled: S3RoleEnabled,
		s3Role:        s3Role,
		accessKeyID:   accessKeyID,
		secretKey:     secretKey,
		bucket:        bucket,
		location:      location,
		prefix:        prefix,
		httpClient: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		awsInstanceMetadataURL: awsInstanceMetadataURL,
		awsCredsRefreshPeriod:  time.Minute,
	}

	err := s3storage.getRoleCredentials()
	if err != nil {
		return nil, err
	}

	return s3storage, nil
}

func (s *Storage) Kind() string {
	return "s3"
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

func (s *Storage) validateName(name string, isFolder bool) error {
	if strings.HasPrefix(name, "/") {
		return ErrInvalidArgumentsNameStartSlash
	}
	if isFolder && name != "" && !strings.HasSuffix(name, "/") {
		// The path must end with `/` so that we don't match entries in parent directory with same prefix name
		// e.g. when scanning /some/entry directory it must not match /some/entry-file object name.
		// That's because in s3, the scan is prefix-based without clear notion of directories.
		return ErrInvalidArgumentsPathNoEndSlash
	}
	if !isFolder && strings.HasSuffix(name, "/") {
		return ErrInvalidArgumentsNameEndSlash
	}
	if strings.Contains(name, "//") {
		return ErrInvalidArgumentsInvalidName
	}
	if strings.Contains("/"+name, "/./") || strings.Contains("/"+name, "/../") {
		return ErrInvalidArgumentsInvalidName
	}
	return nil
}

// Get opens a remote s3 resource
func (s *Storage) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	if offs < 0 || size == 0 {
		return nil, ErrInvalidArgumentsOffsSize
	}
	err := s.validateName(name, false)
	if err != nil {
		return nil, err
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
			return nil, fmt.Errorf("%w: %v", ErrInvalidResponse, err)
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
			reqURL, err = s.parseRedirect(req, resp)
			if err != nil {
				return nil, err
			}

			// Switch to simple GET request
			method = "GET"
			prepareData = func() (io.Reader, string, error) { return nil, "", nil }
			setupRequest = func(req *http.Request) error { return nil }

			log.Printf("S3 %s redirect to GET %s", req.Method, reqURL)

		case 301, 302, 307, 308:
			reqURL, err = s.parseRedirect(req, resp)
			if err != nil {
				return nil, err
			}

			log.Printf("S3 %s redirect to %s", req.Method, reqURL)

		default:
			log.Printf(
				"S3 %s %s failed with status code %d (%s)",
				req.Method,
				req.URL,
				resp.StatusCode,
				resp.Status,
			)
			return nil, fmt.Errorf(
				"%w: request failed with status code %d (%s)",
				ErrInvalidResponse, resp.StatusCode, resp.Status,
			)
		}
	}
	log.Printf("S3 %s %s failed - too many redirects", method, reqURL)
	return nil, ErrTooManyRedirects
}

func (s *Storage) parseRedirect(req *http.Request, resp *http.Response) (string, error) {
	locationURL, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		log.Printf(
			"S3 %s %s failed: invalid `Location` header: '%s' when doing redirection",
			req.Method,
			req.URL,
			req.Header.Get("Location"),
		)
		return "", fmt.Errorf(
			"%w: failed to parse Location header %q: %v",
			ErrInvalidResponse,
			req.Header.Get("Location"),
			err,
		)
	}

	return req.URL.ResolveReference(locationURL).String(), nil
}

// Put writes a remote s3 resource
func (s *Storage) Put(ctx context.Context, name string, fileName string) error {
	err := s.validateName(name, false)
	if err != nil {
		return err
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

func (s *Storage) Remove(ctx context.Context, name string) error {
	err := s.validateName(name, false)
	if err != nil {
		return err
	}

	deleteURL, err := s.originalRequestURL(name)
	if err != nil {
		return err
	}

	resp, err := s.requestWithRedirects(
		ctx,
		"DELETE",
		deleteURL,
		[]int{204},
		func() (io.Reader, string, error) {
			return nil, "", nil
		},
		func(req *http.Request) error { return nil },
	)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (s *Storage) RemoveAll(ctx context.Context, folder string) error {
	err := s.validateName(folder, true)
	if err != nil {
		return err
	}

	entries, subFolders, err := s.ListEntries(ctx, folder)
	if err != nil {
		return err
	}

	for _, e := range entries {
		err := s.Remove(ctx, folder+e.Name)
		if err != nil {
			return err
		}
	}

	for _, subFolder := range subFolders {
		err := s.RemoveAll(ctx, folder+subFolder+"/")
		if err != nil {
			return err
		}
	}
	return nil
}

// Exists checks if a remote resource exists and can be read.
// Note that due to an asynchronous nature of cloud storage,
// a resource stored with the Put method may not be immediately accessible.
func (s *Storage) Exists(ctx context.Context, name string) (bool, error) {
	err := s.validateName(name, false)
	if err != nil {
		return false, err
	}

	entries, _, err := s.scanObjectNames(ctx, name, 1)
	if err != nil {
		return false, err
	}

	// We're looking for all entries with the prefix, since those
	// are sorted alphabetically, if there's an entry with exact
	// name, it would be the first one returned.
	// Since the `scanObjectNames` strips out the path prefix,
	// the entry with the exact name will be returned with an empty name.
	if len(entries) > 0 && entries[0].Name == "" {
		return true, nil
	}

	return false, nil
}

func (s *Storage) ListEntries(ctx context.Context, path string) ([]remotestorage.EntryInfo, []string, error) {
	err := s.validateName(path, true)
	if err != nil {
		return nil, nil, err
	}

	return s.scanObjectNames(ctx, path, 0)
}

func (s *Storage) scanObjectNames(ctx context.Context, prefix string, limit int) ([]remotestorage.EntryInfo, []string, error) {
	prefix = s.prefix + prefix

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
	urlValues.Set("encoding-type", "url")

	if limit > 0 {
		urlValues.Set("max-keys", strconv.Itoa(limit))
	}

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
			return nil, nil, fmt.Errorf("%w: %v", ErrInvalidResponseXmlDecodeError, err)
		}

		for _, object := range respParsed.Contents {
			objectName, err := url.QueryUnescape(object.Key)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: %v", ErrInvalidResponseEntryNameUnescape, err)
			}

			if !strings.HasPrefix(objectName, prefix) {
				return nil, nil, ErrInvalidResponseEntryNameWrongPrefix
			}

			err = s.validateName(objectName, false)
			if err != nil {
				return nil, nil, ErrInvalidResponseEntryNameMalicious
			}

			objectName = strings.TrimPrefix(objectName, prefix)
			if strings.Contains(objectName, "/") {
				return nil, nil, ErrInvalidResponseEntryNameMalicious
			}

			entries = append(entries, remotestorage.EntryInfo{
				Name: strings.TrimPrefix(objectName, prefix),
				Size: object.Size,
			})
		}
		for _, subPath := range respParsed.CommonPrefixes {
			subPathPrefix, err := url.QueryUnescape(subPath.Prefix)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: %v", ErrInvalidResponseSubPathUnescape, err)
			}

			if !strings.HasPrefix(subPathPrefix, prefix) {
				return nil, nil, ErrInvalidResponseSubPathsWrongPrefix
			}
			if !strings.HasSuffix(subPathPrefix, "/") {
				return nil, nil, ErrInvalidResponseSubPathsWrongSuffix
			}

			p := subPathPrefix[len(prefix) : len(subPathPrefix)-1]
			if p == "." || p == ".." || strings.ContainsAny(p, "\\/:") {
				// Avoid exploitation by a malicious server
				return nil, nil, ErrInvalidResponseSubPathMalicious
			}

			subPaths = append(subPaths, p)
		}

		if !respParsed.IsTruncated {
			break
		}

		urlValues.Set("continuation-token", respParsed.NextContinuationToken)
	}

	if !sort.SliceIsSorted(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name }) {
		return nil, nil, ErrInvalidResponseEntriesNotSorted
	}
	if !sort.StringsAreSorted(subPaths) {
		return nil, nil, ErrInvalidResponseSubPathsNotSorted
	}

	return entries, subPaths, nil
}

func (s *Storage) getRoleCredentials() error {
	if !s.S3RoleEnabled {
		return nil
	}

	var err error
	s.accessKeyID, s.secretKey, err = s.requestCredentials()
	if err != nil {
		return err
	}

	s3CredentialsRefreshTicker := time.NewTicker(s.awsCredsRefreshPeriod)
	go func() {
		for {
			select {
			case _ = <-s3CredentialsRefreshTicker.C:
				accessKeyID, secretKey, err := s.requestCredentials()
				if err != nil {
					log.Printf("S3 role credentials lookup failed with an error: %v", err)
					continue
				}
				s.accessKeyID, s.secretKey = accessKeyID, secretKey
			}
		}
	}()

	return nil
}

func (s *Storage) requestCredentials() (string, string, error) {
	tokenReq, err := http.NewRequest("PUT", fmt.Sprintf("%s%s",
		s.awsInstanceMetadataURL,
		"/latest/api/token",
	), nil)
	if err != nil {
		return "", "", errors.New("cannot form metadata token request")
	}

	tokenReq.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")

	tokenResp, err := http.DefaultClient.Do(tokenReq)
	if err != nil {
		return "", "", errors.New("cannot get metadata token")
	}
	defer tokenResp.Body.Close()

	token, err := ioutil.ReadAll(tokenResp.Body)
	if err != nil {
		return "", "", errors.New("cannot read metadata token")
	}

	role := s.s3Role
	if s.s3Role == "" {
		roleReq, err := http.NewRequest("GET", fmt.Sprintf("%s%s",
			s.awsInstanceMetadataURL,
			"/latest/meta-data/iam/info",
		), nil)
		if err != nil {
			return "", "", errors.New("cannot form role name request")
		}

		roleReq.Header.Set("X-aws-ec2-metadata-token", string(token))
		roleResp, err := http.DefaultClient.Do(roleReq)
		if err != nil {
			return "", "", errors.New("cannot get role name")
		}
		defer roleResp.Body.Close()

		creds, err := ioutil.ReadAll(roleResp.Body)
		if err != nil {
			return "", "", errors.New("cannot read role name")
		}

		var metadata struct {
			InstanceProfileArn string `json:"InstanceProfileArn"`
		}
		if err := json.Unmarshal(creds, &metadata); err != nil {
			return "", "", errors.New("cannot parse role name")
		}

		match := arnRoleRegex.FindStringSubmatch(metadata.InstanceProfileArn)
		if len(match) < 2 {
			return "", "", ErrCredentialsCannotBeFound
		}

		role = match[1]
	}

	credsReq, err := http.NewRequest("GET", fmt.Sprintf("%s%s/%s",
		s.awsInstanceMetadataURL,
		"/latest/meta-data/iam/security-credentials",
		role,
	), nil)
	if err != nil {
		return "", "", errors.New("cannot form role credentials request")
	}

	credsReq.Header.Set("X-aws-ec2-metadata-token", string(token))
	credsResp, err := http.DefaultClient.Do(credsReq)
	if err != nil {
		return "", "", errors.New("cannot get role credentials")
	}
	defer credsResp.Body.Close()

	creds, err := ioutil.ReadAll(credsReq.Body)
	if err != nil {
		return "", "", errors.New("cannot read role credentials")
	}

	var credentials struct {
		AccessKeyID     string `json:"AccessKeyId"`
		SecretAccessKey string `json:"SecretAccessKey"`
	}
	if err := json.Unmarshal(creds, &credentials); err != nil {
		return "", "", errors.New("cannot parse role credentials")
	}

	return credentials.AccessKeyID, credentials.SecretAccessKey, nil
}

var _ remotestorage.Storage = (*Storage)(nil)
