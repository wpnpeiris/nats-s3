/*
 * The following code is mostly copied from seaweedfs implementation.
 */

package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/private/protocol/xml/xmlutil"
	"github.com/gorilla/mux"
)

type mimeType string

const (
	mimeNone mimeType = ""
	MimeXML  mimeType = "application/xml"
)

func WriteAwsXMLResponse(w http.ResponseWriter, r *http.Request, statusCode int, result interface{}) {
	var bytesBuffer bytes.Buffer
	err := xmlutil.BuildXML(result, xml.NewEncoder(&bytesBuffer))
	if err != nil {
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	WriteResponse(w, r, statusCode, bytesBuffer.Bytes(), MimeXML)
}

func WriteXMLResponse(w http.ResponseWriter, r *http.Request, statusCode int, response interface{}) {
	WriteResponse(w, r, statusCode, EncodeXMLResponse(response), MimeXML)
}

func WriteEmptyResponse(w http.ResponseWriter, r *http.Request, statusCode int) {
	WriteResponse(w, r, statusCode, []byte{}, mimeNone)
}

func WriteErrorResponse(w http.ResponseWriter, r *http.Request, errorCode ErrorCode) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]
	object = strings.TrimPrefix(object, "/")

	apiError := GetAPIError(errorCode)
	errorResponse := getRESTErrorResponse(apiError, r.URL.Path, bucket, object)
	encodedErrorResponse := EncodeXMLResponse(errorResponse)
	WriteResponse(w, r, apiError.HTTPStatusCode, encodedErrorResponse, MimeXML)
}

func getRESTErrorResponse(err APIError, resource string, bucket, object string) RESTErrorResponse {
	return RESTErrorResponse{
		Code:       err.Code,
		BucketName: bucket,
		Key:        object,
		Message:    err.Description,
		Resource:   resource,
		RequestID:  fmt.Sprintf("%d", time.Now().UnixNano()),
	}
}

// Encodes the response headers into XML format.
func EncodeXMLResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	encoder := xml.NewEncoder(&bytesBuffer)
	err := encoder.Encode(response)
	if err != nil {
		fmt.Printf("Error enconding the response, %s", err)
	}
	return bytesBuffer.Bytes()
}

func setCommonHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("x-amz-request-id", fmt.Sprintf("%d", time.Now().UnixNano()))
	w.Header().Set("Accept-Ranges", "bytes")
	if r.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
}

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
		fmt.Printf("status %d %s: %s", statusCode, mType, string(response))
		_, err := w.Write(response)
		if err != nil {
			fmt.Printf("Error writing the response, %s", err)
		}
		w.(http.Flusher).Flush()
	}
}

func NotFoundHandler(w http.ResponseWriter, r *http.Request) {
	WriteErrorResponse(w, r, ErrMethodNotAllowed)
}
