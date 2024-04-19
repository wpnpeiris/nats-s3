package s3

import (
	"encoding/xml"
	"time"
)

type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Buckets Buckets  `xml:"Buckets"`
	Owner   Owner    `xml:"Owner"`
}

type Buckets struct {
	Bucket []Bucket `xml:"Bucket"`
}

type Bucket struct {
	CreationDate string `xml:"CreationDate"`
	Name         string `xml:"Name"`
}

type Owner struct {
	DisplayName string `xml:"DisplayName"`
	ID          string `xml:"ID"`
}

type ListBucketResult struct {
	IsTruncated bool      `xml:"IsTruncated"`
	Contents    []Content `xml:"Contents"`
	Name        string    `xml:"Name"`
	Prefix      string    `xml:"Prefix"`
	MaxKeys     int       `xml:"MaxKeys"`
}

// Content represents each item in the Contents list
type Content struct {
	ETag         string    `xml:"ETag"`
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	Size         int64     `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
}

// RestoreStatus represents the RestoreStatus element
type RestoreStatus struct {
	IsRestoreInProgress bool      `xml:"IsRestoreInProgress"`
	RestoreExpiryDate   time.Time `xml:"RestoreExpiryDate"`
}

// CommonPrefix represents each Prefix in the CommonPrefixes list
type CommonPrefix struct {
	Prefix string `xml:",chardata"`
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
