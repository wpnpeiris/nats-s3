package s3api

import (
	"encoding/xml"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/s3"
)

type BucketsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   *s3.Owner
	Buckets []*s3.Bucket `xml:"Buckets>Bucket"`
}

func (s *S3Gateway) ListBuckets(w http.ResponseWriter, r *http.Request) {
	nc := s.NATS()

	js, err := nc.JetStream()
	if err != nil {
		handleJetStreamError(err, w)
		return
	}

	entries := js.ObjectStores()

	var buckets []*s3.Bucket

	for entry := range entries {
		buckets = append(buckets, &s3.Bucket{
			Name:         aws.String(entry.Bucket()),
			CreationDate: aws.Time(time.Now())},
		)
	}

	response := BucketsResult{
		Buckets: buckets,
	}

	WriteXMLResponse(w, r, http.StatusOK, response)
}
