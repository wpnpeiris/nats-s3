package client

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/go-kit/log"
	"github.com/nats-io/nats.go"
	"github.com/wpnpeiris/nats-s3/internal/logging"
)

var ErrBucketNotFound = errors.New("bucket not found")
var ErrObjectNotFound = errors.New("object not found")
var ErrUploadNotFound = errors.New("multipart upload not found")
var ErrUploadCompleted = errors.New("completed multipart upload")
var ErrMissingPart = errors.New("missing part")
var ErrBucketAlreadyExists = errors.New("bucket already exists")

type NatsObjectClientOptions struct {
	Replicas int
}

// NatsObjectClient provides convenience helpers for common NATS JetStream
// Object Store operations, built on top of the base Client connection.
type NatsObjectClient struct {
	logger log.Logger
	client *Client
	opts   NatsObjectClientOptions
}

func NewNatsObjectClient(logger log.Logger,
	natsClient *Client,
	opts NatsObjectClientOptions) (*NatsObjectClient, error) {

	if opts.Replicas < 1 {
		logging.Info(logger, "msg", fmt.Sprintf("Invalid replicas given. Will default to 1: %d", opts.Replicas))
		opts.Replicas = 1
	}

	return &NatsObjectClient{
		logger: logger,
		client: natsClient,
		opts:   opts,
	}, nil
}

// IsConnected checks if NATS is connected
func (c *NatsObjectClient) IsConnected() bool {
	nc := c.client.NATS()
	return nc != nil && nc.IsConnected()
}

// Stats returns NATS statistics
func (c *NatsObjectClient) Stats() nats.Statistics {
	nc := c.client.NATS()
	return nc.Stats()
}

// CreateBucket creates a JetStream Object Store bucket.
func (c *NatsObjectClient) CreateBucket(bucketName string) (nats.ObjectStoreStatus, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Create bucket: %s", bucketName))
	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at CreateBucket when nc.JetStream()", "err", err)
		return nil, err
	}

	// Check if bucket already exists to fail duplicate creation explicitly
	_, err = js.ObjectStore(bucketName)
	if err == nil {
		logging.Info(c.logger, "msg", fmt.Sprintf("Bucket already exists: %s", bucketName))
		return nil, ErrBucketAlreadyExists
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		logging.Error(c.logger, "msg", "Unexpected Error at ObjectStore (existence check)", "err", err)
		return nil, err
	}

	os, err := js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket:   bucketName,
		Storage:  nats.FileStorage,
		Replicas: c.opts.Replicas,
	})
	if err != nil {
		logging.Error(c.logger, "msg", "Error at CreateObjectStore", "err", err)
		return nil, err
	}

	return os.Status()
}

// DeleteBucket deletes a bucket identified by its name.
func (c *NatsObjectClient) DeleteBucket(bucket string) error {
	logging.Info(c.logger, "msg", fmt.Sprintf("Delete bucket: %s", bucket))
	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at DeleteBucket", "err", err)
		return err
	}
	err = js.DeleteObjectStore(bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at DeleteBucket", "err", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return ErrBucketNotFound
		}
		return err
	}
	return nil
}

// DeleteObject removes an object identified by bucket and key.
func (c *NatsObjectClient) DeleteObject(bucket string, key string) error {
	logging.Info(c.logger, "msg", fmt.Sprintf("Delete object on bucket: [%s/%s]", bucket, key))
	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at DeleteObject", "err", err)
		return err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at DeleteObject", "err", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return ErrBucketNotFound
		}
		return err
	}
	err = os.Delete(key)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at DeleteObject", "err", err)
		if errors.Is(err, nats.ErrObjectNotFound) {
			return ErrObjectNotFound
		}
		return err
	}

	return nil
}

// GetObjectInfo fetches metadata for an object.
func (c *NatsObjectClient) GetObjectInfo(bucket string, key string) (*nats.ObjectInfo, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Get object info: [%s/%s]", bucket, key))
	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectInfo", "err", err)
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectInfo", "err", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}
	obj, err := os.GetInfo(key)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectInfo", "err", err)
		if errors.Is(err, nats.ErrObjectNotFound) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}

	return obj, err
}

// GetObject retrieves an object's metadata and bytes.
func (c *NatsObjectClient) GetObject(bucket string, key string) (*nats.ObjectInfo, []byte, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Get object : [%s/%s]", bucket, key))
	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObject", "err", err)
		return nil, nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObject", "err", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, nil, ErrBucketNotFound
		}
		return nil, nil, err
	}
	info, err := os.GetInfo(key)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObject", "err", err)
		if errors.Is(err, nats.ErrObjectNotFound) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}
	res, err := os.GetBytes(key)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObject", "err", err)
		return nil, nil, err
	}
	return info, res, nil
}

// ListBuckets returns a channel of object store statuses for all buckets.
func (c *NatsObjectClient) ListBuckets() (<-chan nats.ObjectStoreStatus, error) {
	logging.Info(c.logger, "msg", "List buckets")
	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at ListBuckets", "err", err)
		return nil, err
	}
	return js.ObjectStores(), nil
}

// ListObjects lists all objects in the given bucket.
func (c *NatsObjectClient) ListObjects(bucket string) ([]*nats.ObjectInfo, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("List objects: [%s]", bucket))
	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at ListObjects", "err", err)
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at ListObjects", "err", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}
	ls, err := os.List()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at ListObjects", "err", err)
		if errors.Is(err, nats.ErrNoObjectsFound) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	return ls, err
}

// PutObject writes an object to the given bucket with the provided key and metadata.
func (c *NatsObjectClient) PutObject(bucket string,
	key string,
	contentType string,
	metadata map[string]string,
	data []byte) (*nats.ObjectInfo, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Pub object: [%s/%s]", bucket, key))
	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at PutObject", "err", err)
		return nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at PutObject", "err", err)
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

// GetObjectRetention retrieves retention metadata for an object
func (c *NatsObjectClient) GetObjectRetention(bucket string, key string) (mode string, retainUntilDate string, err error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Get object retention: %s/%s", bucket, key))
	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectRetention when nc.JetStream()", "err", err)
		return "", "", err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectRetention", "err", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return "", "", ErrBucketNotFound
		}
		return "", "", err
	}

	info, err := os.GetInfo(key)
	if err != nil {
		if errors.Is(err, nats.ErrObjectNotFound) {
			return "", "", ErrObjectNotFound
		}
		logging.Error(c.logger, "msg", "Error getting object info", "err", err)
		return "", "", err
	}

	// Check if retention metadata exists
	mode, modeExists := info.Metadata["x-amz-object-lock-mode"]
	retainUntilDate, dateExists := info.Metadata["x-amz-object-lock-retain-until-date"]

	if !modeExists || !dateExists {
		// No retention configuration exists
		return "", "", ErrObjectNotFound
	}

	return mode, retainUntilDate, nil
}

// PutObjectRetention sets retention metadata for an existing object
func (c *NatsObjectClient) PutObjectRetention(bucket string, key string, mode string, retainUntilDate string) error {
	logging.Info(c.logger, "msg", fmt.Sprintf("Put object retention: %s/%s mode=%s until=%s", bucket, key, mode, retainUntilDate))
	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at PutObjectRetention when nc.JetStream()", "err", err)
		return err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at PutObjectRetention", "err", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return ErrBucketNotFound
		}
		return err
	}

	// Get existing object info
	info, err := os.GetInfo(key)
	if err != nil {
		if errors.Is(err, nats.ErrObjectNotFound) {
			return ErrObjectNotFound
		}
		logging.Error(c.logger, "msg", "Error getting object info", "err", err)
		return err
	}

	// Update metadata with retention info
	if info.Metadata == nil {
		info.Metadata = make(map[string]string)
	}
	info.Metadata["x-amz-object-lock-mode"] = mode
	info.Metadata["x-amz-object-lock-retain-until-date"] = retainUntilDate

	// Use UpdateMeta to efficiently update only metadata without touching object data
	meta := &nats.ObjectMeta{
		Name:        info.Name,
		Description: info.Description,
		Metadata:    info.Metadata,
		Headers:     info.Headers,
	}

	err = os.UpdateMeta(key, meta)
	if err != nil {
		logging.Error(c.logger, "msg", "Error updating object metadata", "err", err)
		return err
	}

	return nil
}
