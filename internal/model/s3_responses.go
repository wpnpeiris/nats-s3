/*
 * The following code is mostly copied from seaweedfs implementation.
 */

package model

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type mimeType string

const (
	mimeNone mimeType = ""
	MimeXML  mimeType = "application/xml"
)

// RESTErrorResponse - error response format
type RESTErrorResponse struct {
	XMLName    xml.Name `xml:"Error" json:"-"`
	Code       string   `xml:"Code" json:"Code"`
	Message    string   `xml:"Message" json:"Message"`
	Resource   string   `xml:"Resource" json:"Resource"`
	RequestID  string   `xml:"RequestId" json:"RequestId"`
	Key        string   `xml:"Key,omitempty" json:"Key,omitempty"`
	BucketName string   `xml:"BucketName,omitempty" json:"BucketName,omitempty"`

	// Underlying HTTP status code for the returned error
	StatusCode int `xml:"-" json:"-"`
}

// NewRESTErrorResponse constructs an S3-style XML error body for the given
// API error and request context (resource path, bucket, and object).
func NewRESTErrorResponse(err APIError, resource string, bucket, object string) RESTErrorResponse {
	return RESTErrorResponse{
		Code:       err.Code,
		BucketName: bucket,
		Key:        object,
		Message:    err.Description,
		Resource:   resource,
		RequestID:  fmt.Sprintf("%d", time.Now().UnixNano()),
	}
}

// SetEtag sets the ETag response header. If the provided value is unquoted,
// quotes are added to match S3 behavior.
func SetEtag(w http.ResponseWriter, etag string) {
	if etag != "" {
		if strings.HasPrefix(etag, "\"") {
			w.Header()["ETag"] = []string{etag}
		} else {
			w.Header()["ETag"] = []string{"\"" + etag + "\""}
		}
	}
}

// InitiateMultipartUploadResult wraps the minimal fields returned by S3
// when initiating a multipart upload. It embeds the AWS SDK's response type
// to preserve field names and XML shape.
type InitiateMultipartUploadResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult"`
	s3.CreateMultipartUploadOutput
}

type CompleteMultipartUpload struct {
	Parts []CompletedPart `xml:"Part"`
}

type CompletedPart struct {
	ETag       string
	PartNumber int
}

type ListPartsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult"`

	// copied from s3.ListPartsOutput, the Parts is not converting to <Part></Part>
	Bucket               *string    `type:"string"`
	IsTruncated          *bool      `type:"boolean"`
	Key                  *string    `min:"1" type:"string"`
	MaxParts             *int64     `type:"integer"`
	NextPartNumberMarker *int64     `type:"integer"`
	PartNumberMarker     *int64     `type:"integer"`
	Part                 []*s3.Part `locationName:"Part" type:"list" flattened:"true"`
	StorageClass         *string    `type:"string" enum:"StorageClass"`
	UploadId             *string    `type:"string"`
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult"`
	Location *string  `xml:"Location,omitempty"`
	Bucket   *string  `xml:"Bucket,omitempty"`
	Key      *string  `xml:"Key,omitempty"`
	ETag     *string  `xml:"ETag,omitempty"`
	// VersionId is NOT included in XML body - it should only be in x-amz-version-id HTTP header

	// Store the VersionId internally for setting HTTP header, but don't marshal to XML
	VersionId *string `xml:"-"`
}

// WriteXMLResponse encodes the response as XML and writes it with the given
// HTTP status code and appropriate Content-Type.
func WriteXMLResponse(w http.ResponseWriter, r *http.Request, statusCode int, response interface{}) {
	WriteResponse(w, r, statusCode, EncodeXMLResponse(response), MimeXML)
}

// WriteEmptyResponse writes only headers and the given status code.
func WriteEmptyResponse(w http.ResponseWriter, r *http.Request, statusCode int) {
	WriteResponse(w, r, statusCode, []byte{}, mimeNone)
}

// EncodeXMLResponse serializes a value into an XML byte slice with xml.Header.
func EncodeXMLResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	encoder := xml.NewEncoder(&bytesBuffer)
	err := encoder.Encode(response)
	if err != nil {
		log.Printf("Error enconding the response, %s", err)
	}
	return bytesBuffer.Bytes()
}

// setCommonHeaders sets shared S3-style headers, including a generated
// x-amz-request-id and Accept-Ranges. Also configures permissive CORS if
// the request includes an Origin header.
func setCommonHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("x-amz-request-id", fmt.Sprintf("%d", time.Now().UnixNano()))
	w.Header().Set("Accept-Ranges", "bytes")
	if r.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
}

// WriteErrorResponse looks up the API error for the given code and writes a
// serialized S3 XML error with the appropriate HTTP status.
func WriteErrorResponse(w http.ResponseWriter, r *http.Request, errorCode ErrorCode) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]
	if strings.HasPrefix(object, "/") {
		object = object[1:]
	}

	apiError := GetAPIError(errorCode)
	errorResponse := NewRESTErrorResponse(apiError, r.URL.Path, bucket, object)
	WriteXMLResponse(w, r, apiError.HTTPStatusCode, errorResponse)
}

// WriteResponse writes headers, status code, and optional body, flushing the
// response when done. Body logging is minimized to avoid leaking data.
func WriteResponse(w http.ResponseWriter, r *http.Request, statusCode int, response []byte, mType mimeType) {
	setCommonHeaders(w, r)
	if response != nil {
		w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	}
	if mType != mimeNone {
		w.Header().Set("Content-Type", string(mType))
	}
	w.WriteHeader(statusCode)
	if response != nil {
		log.Printf("status %d %s len=%d", statusCode, mType, len(response))
		_, err := w.Write(response)
		if err != nil {
			log.Printf("Error writing the response, %s", err)
			return
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
}
