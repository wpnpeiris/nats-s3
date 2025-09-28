package s3api

import (
	"encoding/xml"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"net/http"
	"strings"
)

type InitiateMultipartUploadResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult"`
	s3.CreateMultipartUploadOutput
}

// InitiateMultipartUpload initiates S3 multipart upload.
func (s *S3Gateway) InitiateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	uploadID := uuid.New().String()
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	err := s.client.InitMultipartUpload(bucket, key, uploadID)
	if err != nil {
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	response := InitiateMultipartUploadResult{
		CreateMultipartUploadOutput: s3.CreateMultipartUploadOutput{
			Bucket:   aws.String(bucket),
			Key:      objectKey(key),
			UploadId: aws.String(uploadID),
		},
	}
	WriteXMLResponse(w, r, http.StatusOK, response)
}

func objectKey(key string) *string {
	if strings.HasPrefix(key, "/") {
		t := (key)[1:]
		return &t
	}
	return &key
}
