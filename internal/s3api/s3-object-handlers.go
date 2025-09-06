package s3api

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/gorilla/mux"

	"github.com/aws/aws-sdk-go/service/s3"
)

// ListBucketResult is a minimal representation of S3's ListBucket result.
type ListBucketResult struct {
	IsTruncated bool        `xml:"IsTruncated"`
	Contents    []s3.Object `xml:"Contents"`
	Name        string      `xml:"Name"`
	Prefix      string      `xml:"Prefix"`
	MaxKeys     int         `xml:"MaxKeys"`
}

// CopyObjectResult is a compact response shape used by some S3 clients
// to acknowledge a successful object write/copy with an ETag.
type CopyObjectResult struct {
	ETag           string    `xml:"ETag"`
	LastModified   time.Time `xml:"LastModified"`
	ChecksumCRC32  string    `xml:"ChecksumCRC32"`
	ChecksumCRC32C string    `xml:"ChecksumCRC32C"`
	ChecksumSHA1   string    `xml:"ChecksumSHA1"`
	ChecksumSHA256 string    `xml:"ChecksumSHA256"`
}

// ListObjects returns objects in a bucket as a simple S3-compatible XML list.
func (s *S3Gateway) ListObjects(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]

	fmt.Println("List Objects in bucket", bucket)

	nc := s.NATS()
	js, err := nc.JetStream()
	if err != nil {
		handleJetStreamError(err, w)
		return
	}

	os, err := js.ObjectStore(bucket)
	if err != nil {
		handleObjectStoreError(err, w)
		return
	}

	res, err := os.List()
	if err != nil {
		fmt.Printf("Error at Listing bucket, %s", err)
		http.Error(w, "Bucket not found in the ObjectStore", http.StatusNotFound)
		return
	}

	var contents []s3.Object
	for _, obj := range res {
		etag := ""
		if obj.Digest != "" {
			etag = fmt.Sprintf("\"%s\"", obj.Digest)
		}
		contents = append(contents, s3.Object{
			ETag:         aws.String(etag),
			Key:          aws.String(obj.Name),
			LastModified: aws.Time(obj.ModTime),
			Size:         aws.Int64(int64(obj.Size)),
			StorageClass: aws.String(""),
		})
	}

	xmlResponse := ListBucketResult{
		IsTruncated: false,
		Contents:    contents,
		Name:        bucket,
		Prefix:      "",
		MaxKeys:     1000,
	}

	err = xml.NewEncoder(w).Encode(xmlResponse)
	if err != nil {
		fmt.Printf("Error enconding the response, %s", err)
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
	}
}

// Download writes object content to the response and sets typical S3 headers
// such as Last-Modified, ETag, Content-Type, and Content-Length.
func (s *S3Gateway) Download(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	nc := s.NATS()

	js, err := nc.JetStream()
	if err != nil {
		handleJetStreamError(err, w)
		return
	}

	os, err := js.ObjectStore(bucket)
	if err != nil {
		handleObjectStoreError(err, w)
		return
	}

	info, _ := os.GetInfo(key)
	res, err := os.GetBytes(key)
	if err != nil {
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
	}

	if info != nil {
		w.Header().Set("Last-Modified", info.ModTime.UTC().Format(time.RFC1123))
		if info.Digest != "" {
			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", info.Digest))
		}
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(res)))
	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(res)
	if err != nil {
		fmt.Printf("Error writing the response, %s", err)
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
	}
}

// HeadObject writes object metadata headers without a response body.
func (s *S3Gateway) HeadObject(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	nc := s.NATS()

	js, err := nc.JetStream()
	if err != nil {
		handleJetStreamError(err, w)
		return
	}

	os, err := js.ObjectStore(bucket)
	if err != nil {
		handleObjectStoreError(err, w)
		return
	}

	res, err := os.GetInfo(key)
	if err != nil {
		fmt.Printf("Error at  listing object info, %s", err)
		http.Error(w, "Object not found in the bucket", http.StatusNotFound)
		return
	}

	fmt.Printf("Head object %s/%s\n", bucket, key)
	w.Header().Set("Last-Modified", res.ModTime.UTC().Format(time.RFC1123))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", res.Size))
	if res.Digest != "" {
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", res.Digest))
	}
}

// Upload stores an object and responds with 200 and an ETag header.
func (s *S3Gateway) Upload(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	fmt.Println("Upload to", bucket, "with key", key)

	nc := s.NATS()
	js, err := nc.JetStream()
	if err != nil {
		handleJetStreamError(err, w)
		return
	}

	os, err := js.ObjectStore(bucket)
	if err != nil {
		handleObjectStoreError(err, w)
		return
	}

	res, err := os.PutBytes(key, body)
	if err != nil {
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
	}
	if res.Digest != "" {
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", res.Digest))
	}
	WriteEmptyResponse(w, r, http.StatusOK)
}

// DeleteObject deletes the specified object and responds with 204 No Content.
func (s *S3Gateway) DeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	nc := s.NATS()
	js, err := nc.JetStream()
	if err != nil {
		handleJetStreamError(err, w)
		return
	}

	os, err := js.ObjectStore(bucket)
	if err != nil {
		handleObjectStoreError(err, w)
		return
	}

	err = os.Delete(key)
	if err != nil {
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
	}

	WriteEmptyResponse(w, r, http.StatusNoContent)
}
