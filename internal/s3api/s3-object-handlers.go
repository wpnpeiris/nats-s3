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

type ListBucketResult struct {
	IsTruncated bool        `xml:"IsTruncated"`
	Contents    []s3.Object `xml:"Contents"`
	Name        string      `xml:"Name"`
	Prefix      string      `xml:"Prefix"`
	MaxKeys     int         `xml:"MaxKeys"`
}

// CopyObjectResult upload object result
type CopyObjectResult struct {
	ETag           string    `xml:"ETag"`
	LastModified   time.Time `xml:"LastModified"`
	ChecksumCRC32  string    `xml:"ChecksumCRC32"`
	ChecksumCRC32C string    `xml:"ChecksumCRC32C"`
	ChecksumSHA1   string    `xml:"ChecksumSHA1"`
	ChecksumSHA256 string    `xml:"ChecksumSHA256"`
}

func (s3Gateway *S3Gateway) ListObjects(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]

	fmt.Println("List Objects in bucket", bucket)

	nc := s3Gateway.NATS()
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
		contents = append(contents, s3.Object{
			ETag:         aws.String(""),
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
		MaxKeys:     0,
	}

	err = xml.NewEncoder(w).Encode(xmlResponse)
	if err != nil {
		fmt.Printf("Error enconding the response, %s", err)
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
	}
}

func (s3Gateway *S3Gateway) Download(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	nc := s3Gateway.NATS()

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

	res, err := os.GetBytes(key)
	if err != nil {
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
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

func (s3Gateway *S3Gateway) HeadObject(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	nc := s3Gateway.NATS()

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
	w.Header().Set("Last-Modified", res.ModTime.Format(time.RFC3339))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", res.Size))
	if res.Digest != "" {
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", res.Digest))
	}
}

func (s3Gateway *S3Gateway) Upload(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	fmt.Println("Upload to", bucket, "with key", key)

	nc := s3Gateway.NATS()
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

	xmlResponse := CopyObjectResult{
		ETag:           fmt.Sprintf("\"%s\"", res.Digest),
		LastModified:   time.Now(),
		ChecksumCRC32:  "string",
		ChecksumCRC32C: "string",
		ChecksumSHA1:   "string",
		ChecksumSHA256: "string",
	}

	err = xml.NewEncoder(w).Encode(xmlResponse)
	if err != nil {
		fmt.Printf("Error enconding the response, %s", err)
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
	}
}

func (s3Gateway *S3Gateway) DeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	nc := s3Gateway.NATS()
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
