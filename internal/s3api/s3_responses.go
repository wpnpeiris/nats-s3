/*
 * The following code is mostly copied from seaweedfs implementation.
 */

package s3api

import (
	"bytes"
	"encoding/xml"
	"fmt"
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
		}
		w.(http.Flusher).Flush()
	}
}
