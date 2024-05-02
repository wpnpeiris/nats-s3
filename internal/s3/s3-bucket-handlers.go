package s3

import (
	"encoding/xml"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	s3Api "github.com/aws/aws-sdk-go/service/s3"
)

type BucketsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   *s3Api.Owner
	Buckets []*s3Api.Bucket `xml:"Buckets>Bucket"`
}

func (s3Gateway *S3Gateway) ListBuckets(w http.ResponseWriter, r *http.Request) {
	nc := s3Gateway.NATS()

	js, _ := nc.JetStream()
	entries := js.ObjectStores()

	var buckets []*s3Api.Bucket

	for entry := range entries {
		buckets = append(buckets, &s3Api.Bucket{
			Name:         aws.String(entry.Bucket()),
			CreationDate: aws.Time(time.Now())},
		)
	}

	response := BucketsResult{
		Buckets: buckets,
	}

	WriteXMLResponse(w, r, http.StatusOK, response)
}
