package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

var ErrBucketNotFound = errors.New("bucket not found")
var ErrObjectNotFound = errors.New("object not found")

// PartMeta describes a single part in a multipart upload session.
// It records the part number, ETag (checksum), size in bytes, and the
// time the part was stored (Unix seconds).
type PartMeta struct {
	Number   int    `json:"number"`
	ETag     string `json:"etag"`
	Size     int64  `json:"size"`
	StoredAt int64  `json:"stored_at_unix"`
}

// UploadMeta captures the server-side state of a multipart upload session.
// It includes identifiers (UploadID, Bucket, Key), initiation time (UTC),
// optional owner, constraints (minimum part size and max parts), the
// collected parts, and completion/abort flags. The JSON value is persisted
// in a Key-Value store under a session-specific key.
type UploadMeta struct {
	UploadID  string           `json:"upload_id"`
	Bucket    string           `json:"bucket"`
	Key       string           `json:"key"`
	Initiated time.Time        `json:"initiated"`
	Owner     string           `json:"owner,omitempty"` // optional, auth principal
	MinPartSz int64            `json:"min_part_size"`   // default 5MiB
	MaxParts  int              `json:"max_parts"`       // default 10000
	Parts     map[int]PartMeta `json:"parts"`
	Completed bool             `json:"completed"`
	Aborted   bool             `json:"aborted"`
}

// MultiPartStore groups storage backends used for multipart uploads.
// SessionStore tracks session metadata in a Key-Value bucket, while
// TempPartStore holds uploaded parts in a temporary Object Store.
type MultiPartStore struct {
	SessionStore  nats.KeyValue
	TempPartStore nats.ObjectStore
}

// createSession persists the given session value at the provided key in the
// session Key-Value store. The value is expected to be a JSON-encoded
// UploadMeta blob. Returns any error encountered during the put operation.
func (m *MultiPartStore) createSession(sessionKey string, sessionValue []byte) error {
	_, err := m.SessionStore.Put(sessionKey, sessionValue)
	if err != nil {
		log.Printf("Error at createSession when kv.Put(): %v\n", err)
		return err
	}
	return nil
}

// NatsObjectClient provides convenience helpers for common NATS JetStream
// Object Store operations, built on top of the base Client connection.
type NatsObjectClient struct {
	Client         *Client
	MultiPartStore *MultiPartStore
}

// CreateBucket creates a JetStream Object Store bucket.
func (c *NatsObjectClient) CreateBucket(bucketName string) (nats.ObjectStoreStatus, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at CreateBucket when nc.JetStream(): %v\n", err)
		return nil, err
	}

	os, err := js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket:  bucketName,
		Storage: nats.FileStorage,
	})
	if err != nil {
		log.Printf("Error at CreateBucket when js.CreateObjectStore: %v\n", err)
		return nil, err
	}

	return os.Status()
}

// DeleteBucket delete an bucket identified by bucket name.
func (c *NatsObjectClient) DeleteBucket(bucket string) error {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at DeleteBucket when nc.JetStream(): %v\n", err)
		return err
	}
	err = js.DeleteObjectStore(bucket)
	if err != nil {
		log.Printf("Error at DeleteBucket when js.DeleteObjectStore(): %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return ErrBucketNotFound
		}
		return err
	}
	return nil
}

// DeleteObject removes an object identified by bucket and key.
func (c *NatsObjectClient) DeleteObject(bucket string, key string) error {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at DeleteObject when nc.JetStream(): %v\n", err)
		return err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at DeleteObject when js.ObjectStore: %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return ErrBucketNotFound
		}
		return err
	}
	err = os.Delete(key)
	if err != nil {
		log.Printf("Error at DeleteObject when os.Delete: %v\n", err)
		if errors.Is(err, nats.ErrObjectNotFound) {
			return ErrObjectNotFound
		}
		return err
	}

	return nil
}

// GetObjectInfo fetches metadata for an object.
func (c *NatsObjectClient) GetObjectInfo(bucket string, key string) (*nats.ObjectInfo, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at GetObjectInfo when js.JetStream: %v\n", err)
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at GetObjectInfo when js.ObjectStore: %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}
	obj, err := os.GetInfo(key)
	if err != nil {
		log.Printf("Error at GetObjectInfo when os.GetInfo: %v\n", err)
		if errors.Is(err, nats.ErrObjectNotFound) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}

	return obj, err
}

// GetObject retrieves an object and its metadata.
func (c *NatsObjectClient) GetObject(bucket string, key string) (*nats.ObjectInfo, []byte, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at GetObjectInfo when js.JetStream: %v\n", err)
		return nil, nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at GetObjectInfo when js.ObjectStore: %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, nil, ErrBucketNotFound
		}
		return nil, nil, err
	}
	info, err := os.GetInfo(key)
	if err != nil {
		log.Printf("Error at GetObjectInfo when os.GetInfo: %v\n", err)
		if errors.Is(err, nats.ErrObjectNotFound) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}
	res, err := os.GetBytes(key)
	if err != nil {
		log.Printf("Error at GetObjectInfo when os.GetBytes: %v\n", err)
		return nil, nil, err
	}
	return info, res, nil
}

// ListBuckets returns a channel of object store statuses for all buckets.
func (c *NatsObjectClient) ListBuckets() (<-chan nats.ObjectStoreStatus, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at ListBuckets when js.JetStream: %v\n", err)
		return nil, err
	}
	return js.ObjectStores(), nil
}

// ListObjects lists all objects in the given bucket.
func (c *NatsObjectClient) ListObjects(bucket string) ([]*nats.ObjectInfo, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at ListObjects when js.JetStream: %v\n", err)
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at ListObjects when js.ObjectStore: %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}
	ls, err := os.List()
	if err != nil {
		log.Printf("Error at ListObjects when js.List: %v\n", err)
		if errors.Is(err, nats.ErrNoObjectsFound) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	return ls, err
}

// PutObject writes an object to the given bucket with the provided key and metadata
func (c *NatsObjectClient) PutObject(bucket string,
	key string,
	contentType string,
	metadata map[string]string,
	data []byte) (*nats.ObjectInfo, error) {
	nc := c.Client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		log.Printf("Error at PutObject when js.JetStream: %v\n", err)
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		log.Printf("Error at PutObject when js.ObjectStore: %v\n", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}

	meta := nats.ObjectMeta{
		Name:     key,
		Metadata: metadata,
		Headers: nats.Header{
			"Content-Type": []string{contentType},
		},
	}

	return os.Put(&meta, bytes.NewReader(data))
}

func (c *NatsObjectClient) InitMultipartUpload(bucket string, key string, uploadID string) error {
	meta := UploadMeta{
		UploadID:  uploadID,
		Bucket:    bucket,
		Key:       key,
		Initiated: time.Now().UTC(),
		MinPartSz: 5 * 1024 * 1024,
		MaxParts:  10000,
		Parts:     map[int]PartMeta{},
	}
	data, err := json.Marshal(meta)
	if err != nil {
		log.Printf("Error at InitMultipartUpload when json.Marshal(): %v\n", err)
		return err
	}
	sessionKey := sessionKey(bucket, key, uploadID)
	return c.MultiPartStore.createSession(sessionKey, data)
}

func sessionKey(bucket, key, uploadID string) string {
	return fmt.Sprintf("multi_parts/%s/%s/%s", bucket, key, uploadID)
}
