package s3

import (
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

type Server struct {
	*Component
}

func (s *Server) Upload(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	fmt.Println("Upload to", bucket, "with key", key)

	nc := s.NATS()
	js, _ := nc.JetStream()
	os, _ := js.ObjectStore(bucket)

	res, err := os.PutBytes(key, body)
	if err != nil {
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
	}

	xmlResponse := CopyObjectResult{
		ETag:           res.Name,
		LastModified:   time.Now(),
		ChecksumCRC32:  "string",
		ChecksumCRC32C: "string",
		ChecksumSHA1:   "string",
		ChecksumSHA256: "string",
	}

	xml.NewEncoder(w).Encode(xmlResponse)
}

func (s *Server) ListObjects(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]

	fmt.Println("List Objects in bucket", bucket)

	nc := s.NATS()
	js, _ := nc.JetStream()
	os, _ := js.ObjectStore(bucket)

	res, err := os.List()
	var xmlContents []Content
	for _, obj := range res {
		xmlContents = append(xmlContents, Content{
			ETag:         "",
			Key:          obj.Name,
			LastModified: obj.ModTime,
			Size:         int64(obj.Size),
			StorageClass: "",
		})
	}
	if err != nil {
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
	}

	xmlResponse := ListBucketResult{
		IsTruncated: false,
		Contents:    xmlContents,
		Name:        "Filename",
		Prefix:      "",
		MaxKeys:     0,
	}

	xml.NewEncoder(w).Encode(xmlResponse)
}

func (s *Server) ListBuckets(w http.ResponseWriter, _ *http.Request) {
	nc := s.NATS()

	js, _ := nc.JetStream()
	buckets := js.ObjectStores()

	var formattedBuckets []Bucket
	for bucket := range buckets {
		formattedBuckets = append(formattedBuckets,
			Bucket{Name: bucket.Bucket(),
				CreationDate: "2023-10-11T20:00:00.000000Z"},
		)
	}

	xmlResponse := ListAllMyBucketsResult{
		Buckets: Buckets{Bucket: formattedBuckets},
		Owner:   Owner{DisplayName: "DisplayNameGoesHere", ID: "YourIDGoesHere"},
	}
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(xmlResponse)
}

func (s *Server) HeadObject(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	nc := s.NATS()

	js, _ := nc.JetStream()

	os, _ := js.ObjectStore(bucket)
	res, err := os.GetInfo(key)
	if err != nil {
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
	}

	fmt.Printf("Head object %s/%s\n", bucket, key)
	w.Header().Set("Last-Modified", res.ModTime.Format(time.RFC3339))
	w.Header().Set("Content-Length", strconv.FormatUint(res.Size, 10))
}

func (s *Server) Download(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	key := mux.Vars(r)["key"]

	nc := s.NATS()

	js, _ := nc.JetStream()

	os, _ := js.ObjectStore(bucket)
	res, err := os.GetBytes(key)
	if err != nil {
		http.Error(w, "Unexpected", http.StatusInternalServerError)
		return
	}

	w.Write(res)
}

func (s *Server) ListenAndServe(addr string) error {

	r := mux.NewRouter()
	router := r.PathPrefix("/").Subrouter()

	router.HandleFunc("/{bucket}/{key}", s.Download).Methods("GET")
	router.HandleFunc("/{bucket}/{key}", s.HeadObject).Methods("HEAD")
	router.HandleFunc("/{bucket}/{key}", s.Upload).Methods("PUT")

	router.HandleFunc("/{bucket}", s.ListObjects).Methods("GET")
	router.HandleFunc("/", s.ListBuckets).Methods("GET")

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:           addr,
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go srv.Serve(l)

	return nil
}
