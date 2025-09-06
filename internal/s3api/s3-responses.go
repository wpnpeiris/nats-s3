/*
 * The following code is mostly copied from seaweedfs implementation.
 */

package s3api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

type mimeType string

const (
	mimeNone mimeType = ""
	MimeXML  mimeType = "application/xml"
)

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
		fmt.Printf("Error enconding the response, %s", err)
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
		fmt.Printf("status %d %s len=%d\n", statusCode, mType, len(response))
		_, err := w.Write(response)
		if err != nil {
			fmt.Printf("Error writing the response, %s", err)
		}
		w.(http.Flusher).Flush()
	}
}
