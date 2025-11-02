package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/go-kit/log"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

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
	ctx    context.Context
	logger log.Logger
	client *Client
	opts   NatsObjectClientOptions
}

func NewNatsObjectClient(ctx context.Context,
	logger log.Logger,
	natsClient *Client,
	opts NatsObjectClientOptions) (*NatsObjectClient, error) {

	if opts.Replicas < 1 {
		logging.Info(logger, "msg", fmt.Sprintf("Invalid replicas given. Will default to 1: %d", opts.Replicas))
		opts.Replicas = 1
	}

	return &NatsObjectClient{
		ctx:    ctx,
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
func (c *NatsObjectClient) CreateBucket(ctx context.Context, bucketName string) (jetstream.ObjectStoreStatus, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Create bucket: %s", bucketName))
	js := c.client.JetStream()

	// Check if bucket already exists to fail duplicate creation explicitly
	_, err := js.ObjectStore(ctx, bucketName)
	if err == nil {
		logging.Info(c.logger, "msg", fmt.Sprintf("Bucket already exists: %s", bucketName))
		return nil, ErrBucketAlreadyExists
	} else if !errors.Is(err, jetstream.ErrBucketNotFound) {
		logging.Error(c.logger, "msg", "Unexpected Error at ObjectStore (existence check)", "err", err)
		return nil, err
	}

	os, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:   bucketName,
		Replicas: c.opts.Replicas,
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		logging.Error(c.logger, "msg", "Error at CreateObjectStore", "err", err)
		return nil, err
	}

	return os.Status(ctx)
}

// DeleteBucket deletes a bucket identified by its name.
func (c *NatsObjectClient) DeleteBucket(ctx context.Context, bucket string) error {
	logging.Info(c.logger, "msg", fmt.Sprintf("Delete bucket: %s", bucket))
	js := c.client.JetStream()
	err := js.DeleteObjectStore(ctx, bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at DeleteBucket", "err", err)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return ErrBucketNotFound
		}
		return err
	}
	return nil
}

// DeleteObject removes an object identified by bucket and key.
func (c *NatsObjectClient) DeleteObject(ctx context.Context, bucket string, key string) error {
	logging.Info(c.logger, "msg", fmt.Sprintf("Delete object on bucket: [%s/%s]", bucket, key))
	js := c.client.JetStream()
	os, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at DeleteObject", "err", err)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return ErrBucketNotFound
		}
		return err
	}
	err = os.Delete(ctx, key)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at DeleteObject", "err", err)
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return ErrObjectNotFound
		}
		return err
	}

	return nil
}

// GetObjectInfo fetches metadata for an object.
func (c *NatsObjectClient) GetObjectInfo(ctx context.Context, bucket string, key string) (*jetstream.ObjectInfo, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Get object info: [%s/%s]", bucket, key))
	js := c.client.JetStream()
	os, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectInfo", "err", err)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}
	obj, err := os.GetInfo(ctx, key)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectInfo", "err", err)
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}

	return obj, err
}

// GetObject retrieves an object's metadata and bytes.
func (c *NatsObjectClient) GetObject(ctx context.Context, bucket string, key string) (*jetstream.ObjectInfo, []byte, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Get object : [%s/%s]", bucket, key))
	js := c.client.JetStream()
	os, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObject", "err", err)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, nil, ErrBucketNotFound
		}
		return nil, nil, err
	}
	info, err := os.GetInfo(ctx, key)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObject", "err", err)
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}
	res, err := os.GetBytes(ctx, key)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObject", "err", err)
		return nil, nil, err
	}
	return info, res, nil
}

// ListBuckets returns a channel of object store statuses for all buckets.
func (c *NatsObjectClient) ListBuckets(ctx context.Context) (<-chan jetstream.ObjectStoreStatus, error) {
	logging.Info(c.logger, "msg", "List buckets")
	js := c.client.JetStream()
	return js.ObjectStores(ctx).Status(), nil
}

// ListObjects lists all objects in the given bucket.
func (c *NatsObjectClient) ListObjects(ctx context.Context, bucket string) ([]*jetstream.ObjectInfo, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("List objects: [%s]", bucket))
	js := c.client.JetStream()
	os, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at ListObjects", "err", err)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}
	ls, err := os.List(ctx)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at ListObjects", "err", err)
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	return ls, err
}

// PutObject writes an object to the given bucket with the provided key and metadata.
func (c *NatsObjectClient) PutObject(ctx context.Context, bucket string,
	key string,
	contentType string,
	metadata map[string]string,
	data []byte) (*jetstream.ObjectInfo, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Pub object: [%s/%s]", bucket, key))
	js := c.client.JetStream()
	os, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at PutObject", "err", err)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}

	meta := jetstream.ObjectMeta{
		Name:     key,
		Metadata: metadata,
		Headers: nats.Header{
			"Content-Type": []string{contentType},
		},
	}

	return os.Put(ctx, meta, bytes.NewReader(data))
}

// GetObjectRetention retrieves retention metadata for an object
func (c *NatsObjectClient) GetObjectRetention(ctx context.Context, bucket string, key string) (mode string, retainUntilDate string, err error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Get object retention: %s/%s", bucket, key))
	js := c.client.JetStream()
	os, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectRetention", "err", err)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return "", "", ErrBucketNotFound
		}
		return "", "", err
	}

	info, err := os.GetInfo(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
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
func (c *NatsObjectClient) PutObjectRetention(ctx context.Context, bucket string, key string, mode string, retainUntilDate string) error {
	logging.Info(c.logger, "msg", fmt.Sprintf("Put object retention: %s/%s mode=%s until=%s", bucket, key, mode, retainUntilDate))
	js := c.client.JetStream()
	os, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at PutObjectRetention", "err", err)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return ErrBucketNotFound
		}
		return err
	}

	// Get existing object info
	info, err := os.GetInfo(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
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
	meta := jetstream.ObjectMeta{
		Name:        info.Name,
		Description: info.Description,
		Metadata:    info.Metadata,
		Headers:     info.Headers,
	}

	err = os.UpdateMeta(ctx, key, meta)
	if err != nil {
		logging.Error(c.logger, "msg", "Error updating object metadata", "err", err)
		return err
	}

	return nil
}
