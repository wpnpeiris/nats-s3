package client

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-kit/log"
	"github.com/wpnpeiris/nats-s3/internal/logging"
	"io"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

var ErrBucketNotFound = errors.New("bucket not found")
var ErrObjectNotFound = errors.New("object not found")
var ErrUploadNotFound = errors.New("multipart upload not found")
var ErrUploadCompleted = errors.New("completed multipart upload")
var ErrMissingPart = errors.New("missing part")
var ErrBucketAlreadyExists = errors.New("bucket already exists")

// PartMeta describes a single part in a multipart upload session.
// It records the part number, ETag (checksum), size in bytes, and the
// time the part was stored (Unix seconds).
type PartMeta struct {
	Number   int    `json:"number"`
	ETag     string `json:"etag"`
	Size     uint64 `json:"size"`
	StoredAt int64  `json:"stored_at_unix"`
}

// UploadMeta captures the server-side state of a multipart upload session.
// It includes identifiers (UploadID, Bucket, Key), initiation time (UTC),
// optional owner, and constraints (minimum part size and max parts).
// The JSON value is persisted in a Key-Value store under a session-specific key.
// Individual part metadata is stored in separate KV entries to avoid write conflicts.
// The Parts field is populated on-demand when calling ListParts or CompleteMultipartUpload.
type UploadMeta struct {
	UploadID  string           `json:"upload_id"`
	Bucket    string           `json:"bucket"`
	Key       string           `json:"key"`
	Initiated time.Time        `json:"initiated"`
	Owner     string           `json:"owner,omitempty"` // optional, auth principal
	MinPartSz int64            `json:"min_part_size"`   // default 5MiB
	MaxParts  int              `json:"max_parts"`       // default 10000
	Parts     map[int]PartMeta `json:"-"`               // Not persisted, populated on-demand
}

// MultiPartStore groups storage backends used for multipart uploads.
// sessionStore tracks session metadata in a Key-Value bucket, while
// tempPartStore holds uploaded parts in a temporary Object Store.
type MultiPartStore struct {
	logger        log.Logger
	sessionStore  nats.KeyValue
	tempPartStore nats.ObjectStore
}

func newMultiPartStore(logger log.Logger, c *Client) (*MultiPartStore, error) {
	nc := c.NATS()
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to create multipart session store when calling nc.JetStream(): %w", err)
	}

	kv, err := js.KeyValue(MultiPartSessionStoreName)
	if err != nil {
		if errors.Is(err, nats.ErrBucketNotFound) {
			kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket: MultiPartSessionStoreName,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create multipart session store when calling js.CreateKeyValue(): %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to access multipart session store when calling js.KeyValue(): %w", err)
		}
	}

	os, err := js.ObjectStore(MultiPartTempStoreName)
	if err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			os, err = js.CreateObjectStore(&nats.ObjectStoreConfig{
				Bucket: MultiPartTempStoreName,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create multipart temp store when calling js.CreateObjectStore(): %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to access multipart temp store when calling js.ObjectStore(): %w", err)
		}
	}

	return &MultiPartStore{
		logger:        logger,
		sessionStore:  kv,
		tempPartStore: os,
	}, nil
}

// createUploadMeta persists the given session value at the provided key in the
// session Key-Value store. The value is expected to be a JSON-encoded
// UploadMeta blob. Returns any error encountered during the put operation.
func (m *MultiPartStore) createUploadMeta(meta UploadMeta) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("creating upload meta: %v", meta))
	data, err := json.Marshal(meta)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at createUploadMeta when json.Marshal()", "err", err)
		return err
	}
	key := sessionKey(meta.Bucket, meta.Key, meta.UploadID)
	_, err = m.sessionStore.Put(key, data)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at createUploadMeta when sessionStore.Put()", "err", err)
		return err
	}
	return nil
}

// saveUploadMeta updates the persisted multipart upload session metadata.
func (m *MultiPartStore) saveUploadMeta(meta UploadMeta, revision uint64) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("save upload meta: %v", meta))
	data, err := json.Marshal(meta)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at saveUploadMeta when json.Marshal()", "err", err)
		return err
	}
	key := sessionKey(meta.Bucket, meta.Key, meta.UploadID)
	// FIXME: Retries on revision conflicts to synchronize concurrent updates across goroutines/processes.
	_, err = m.sessionStore.Update(key, data, revision)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at saveUploadMeta when sessionStore.Update()", "err", err)
		return err
	}
	return nil
}

// deleteUploadMeta delete the persisted multipart upload session metadata.
func (m *MultiPartStore) deleteUploadMeta(meta UploadMeta) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("deleting upload meta: %v", meta))
	key := sessionKey(meta.Bucket, meta.Key, meta.UploadID)
	err := m.sessionStore.Delete(key)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at deleteUploadMeta when sessionStore.Delete()", "err", err)
		return err
	}

	return nil
}

// createPartUpload streams a part from the provided reader into the temporary
// Object Store under the given part key and returns the stored object's info.
func (m *MultiPartStore) createPartUpload(partKey string, dataReader *io.PipeReader) (*nats.ObjectInfo, error) {
	logging.Debug(m.logger, "msg", fmt.Sprintf("uploading part: %s", partKey))
	obj, err := m.tempPartStore.Put(&nats.ObjectMeta{Name: partKey}, dataReader)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at createPartUpload when tempPartStore.Put()", "err", err)
		return nil, err
	}
	return obj, nil
}

// getPartUpload return part from the temporary Object Store.
func (m *MultiPartStore) getPartUpload(partKey string) (nats.ObjectResult, error) {
	logging.Debug(m.logger, "msg", fmt.Sprintf("get part upload: %s", partKey))
	return m.tempPartStore.Get(partKey)
}

// savePartMeta stores metadata for a single part in the KV store.
// Each part gets its own KV entry to avoid write conflicts during concurrent uploads.
func (m *MultiPartStore) savePartMeta(bucket, key, uploadID string, partMeta PartMeta) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("save part meta: bucket=%s key=%s uploadID=%s part=%d", bucket, key, uploadID, partMeta.Number))
	data, err := json.Marshal(partMeta)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at savePartMeta when json.Marshal()", "err", err)
		return err
	}
	partMetaKey := partMetaKey(bucket, key, uploadID, partMeta.Number)
	_, err = m.sessionStore.Put(partMetaKey, data)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at savePartMeta when sessionStore.Put()", "err", err)
		return err
	}
	return nil
}

// getPartMeta retrieves metadata for a single part from the KV store.
func (m *MultiPartStore) getPartMeta(bucket, key, uploadID string, partNumber int) (*PartMeta, error) {
	logging.Debug(m.logger, "msg", fmt.Sprintf("get part meta: bucket=%s key=%s uploadID=%s part=%d", bucket, key, uploadID, partNumber))
	partMetaKey := partMetaKey(bucket, key, uploadID, partNumber)
	entry, err := m.sessionStore.Get(partMetaKey)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at getPartMeta when sessionStore.Get()", "err", err)
		return nil, err
	}
	var partMeta PartMeta
	if err := json.Unmarshal(entry.Value(), &partMeta); err != nil {
		logging.Error(m.logger, "msg", "Error at getPartMeta when json.Unmarshal()", "err", err)
		return nil, err
	}
	return &partMeta, nil
}

// getAllPartMeta retrieves all part metadata for a given upload session.
func (m *MultiPartStore) getAllPartMeta(bucket, key, uploadID string) (map[int]PartMeta, error) {
	logging.Debug(m.logger, "msg", fmt.Sprintf("get all part meta: bucket=%s key=%s uploadID=%s", bucket, key, uploadID))
	prefix := partMetaPrefix(bucket, key, uploadID)

	keys, err := m.sessionStore.Keys()
	if err != nil {
		logging.Error(m.logger, "msg", "Error at getAllPartMeta when sessionStore.Keys()", "err", err)
		return nil, err
	}

	parts := make(map[int]PartMeta)
	for _, kvKey := range keys {
		if strings.HasPrefix(kvKey, prefix) {
			entry, err := m.sessionStore.Get(kvKey)
			if err != nil {
				logging.Warn(m.logger, "msg", "Error getting part metadata", "key", kvKey, "err", err)
				continue
			}
			var partMeta PartMeta
			if err := json.Unmarshal(entry.Value(), &partMeta); err != nil {
				logging.Warn(m.logger, "msg", "Error unmarshaling part metadata", "key", kvKey, "err", err)
				continue
			}
			parts[partMeta.Number] = partMeta
		}
	}

	return parts, nil
}

// deleteAllPartMeta deletes all part metadata entries for a given upload session.
func (m *MultiPartStore) deleteAllPartMeta(bucket, key, uploadID string) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("delete all part meta: bucket=%s key=%s uploadID=%s", bucket, key, uploadID))
	prefix := partMetaPrefix(bucket, key, uploadID)

	keys, err := m.sessionStore.Keys()
	if err != nil {
		logging.Error(m.logger, "msg", "Error at deleteAllPartMeta when sessionStore.Keys()", "err", err)
		return err
	}

	for _, kvKey := range keys {
		if strings.HasPrefix(kvKey, prefix) {
			err := m.sessionStore.Delete(kvKey)
			if err != nil {
				logging.Warn(m.logger, "msg", "Error deleting part metadata", "key", kvKey, "err", err)
			}
		}
	}

	return nil
}

// deletePartUpload delete part from the temporary Object Store.
func (m *MultiPartStore) deletePartUpload(partKey string) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("delete part upload: %s", partKey))
	return m.tempPartStore.Delete(partKey)
}

// deleteAllPartUpload delete all parts from the temporary Object Store.
func (m *MultiPartStore) deleteAllPartUpload(bucket, key, uploadID string, parts map[int]PartMeta) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("delete all part upload: bucket=%s key=%s uploadID=%s", bucket, key, uploadID))
	for pn := range parts {
		partKey := partKey(bucket, key, uploadID, pn)
		err := m.tempPartStore.Delete(partKey)
		if err != nil {
			logging.Error(m.logger, "msg", "Error at deleteAllPartUpload when tempPartStore.Delete()", "err", err)
			return err
		}
	}

	return nil
}

// getUploadMeta fetches the KV entry for a multipart upload session, including
// its current revision number for optimistic updates.
func (m *MultiPartStore) getUploadMeta(sessionKey string) (nats.KeyValueEntry, error) {
	logging.Debug(m.logger, "msg", fmt.Sprintf("get upload meta: %s", sessionKey))
	entry, err := m.sessionStore.Get(sessionKey)
	if err != nil {
		logging.Error(m.logger, "msg", "Error at getSession when kv.Get()", "err", err)
		return nil, err
	}
	return entry, nil
}

// NatsObjectClient provides convenience helpers for common NATS JetStream
// Object Store operations, built on top of the base Client connection.
type NatsObjectClient struct {
	logger         log.Logger
	client         *Client
	multiPartStore *MultiPartStore
}

func NewNatsObjectClient(logger log.Logger, natsClient *Client) (*NatsObjectClient, error) {
	mps, err := newMultiPartStore(logger, natsClient)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize multipart store: %w", err)
	}
	return &NatsObjectClient{
		logger:         logger,
		client:         natsClient,
		multiPartStore: mps,
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
		Bucket:  bucketName,
		Storage: nats.FileStorage,
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
	logging.Info(c.logger, "msg", fmt.Sprintf("Delete object : [%s/%s]", bucket, key))
	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectInfo", "err", err)
		return nil, nil, err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectInfo", "err", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, nil, ErrBucketNotFound
		}
		return nil, nil, err
	}
	info, err := os.GetInfo(key)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectInfo", "err", err)
		if errors.Is(err, nats.ErrObjectNotFound) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}
	res, err := os.GetBytes(key)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at GetObjectInfo", "err", err)
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

// InitMultipartUpload creates and persists a new multipart upload session
// for the given bucket/key and uploadID.
func (c *NatsObjectClient) InitMultipartUpload(bucket string, key string, uploadID string) error {
	logging.Info(c.logger, "msg", fmt.Sprintf("Init multipart upload: [%s/%s]", bucket, key))
	meta := UploadMeta{
		UploadID:  uploadID,
		Bucket:    bucket,
		Key:       key,
		Initiated: time.Now().UTC(),
		MinPartSz: 5 * 1024 * 1024,
		MaxParts:  10000,
		// Parts is not initialized - stored separately in KV to avoid write conflicts
	}

	return c.multiPartStore.createUploadMeta(meta)
}

// UploadPart streams a part into temporary storage and records its ETag/size
// under the multipart session. Returns the hex ETag (without quotes).
func (c *NatsObjectClient) UploadPart(bucket string, key string, uploadID string, part int, dataReader io.ReadCloser) (string, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Upload part:%06d [%s/%s], UploadID: %s", part, bucket, key, uploadID))
	sessionKey := sessionKey(bucket, key, uploadID)
	sessionData, err := c.multiPartStore.getUploadMeta(sessionKey)
	if err != nil {
		return "", ErrUploadNotFound
	}

	var meta UploadMeta
	if err := json.Unmarshal(sessionData.Value(), &meta); err != nil {
		logging.Error(c.logger, "msg", "Error at UploadPart", "err", err)
		return "", err
	}

	h := md5.New()
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		_, err := io.Copy(io.MultiWriter(h, pw), dataReader)
		if err != nil {
			_ = pw.CloseWithError(err)
		}
	}()

	partKey := partKey(bucket, key, uploadID, part)
	obj, err := c.multiPartStore.createPartUpload(partKey, pr)
	if err != nil {
		// Close the reader to signal the goroutine to stop
		// This prevents goroutine leak if createPartUpload fails
		_ = pr.Close()
		return "", err
	}

	etag := strings.ToLower(hex.EncodeToString(h.Sum(nil)))
	partMeta := PartMeta{
		Number: part, ETag: `"` + etag + `"`, Size: obj.Size, StoredAt: time.Now().Unix(),
	}

	// Save part metadata in its own KV entry to avoid write conflicts
	err = c.multiPartStore.savePartMeta(bucket, key, uploadID, partMeta)
	if err != nil {
		return "", err
	}
	return etag, nil
}

// AbortMultipartUpload aborts an in‑progress multipart upload, deleting any
// uploaded parts and removing the session metadata.
func (c *NatsObjectClient) AbortMultipartUpload(bucket string, key string, uploadID string) error {
	logging.Info(c.logger, "msg", fmt.Sprintf("Abort multipart upload: [%s/%s], UploadID: %s", bucket, key, uploadID))
	sessionKey := sessionKey(bucket, key, uploadID)
	sessionData, err := c.multiPartStore.getUploadMeta(sessionKey)
	if err != nil {
		return ErrUploadNotFound
	}

	var meta UploadMeta
	if err := json.Unmarshal(sessionData.Value(), &meta); err != nil {
		logging.Error(c.logger, "msg", "Error at AbortMultipartUpload", "err", err)
		return err
	}

	// Get all part metadata to find all parts to delete
	parts, err := c.multiPartStore.getAllPartMeta(bucket, key, uploadID)
	if err != nil {
		logging.Warn(c.logger, "msg", "Error getting part metadata at AbortMultipartUpload", "err", err)
		// Continue with cleanup even if we can't get all parts
		parts = make(map[int]PartMeta)
	}

	// Delete temporary part data from Object Store
	for pn := range parts {
		partKey := partKey(bucket, key, uploadID, pn)
		err := c.multiPartStore.deletePartUpload(partKey)
		if err != nil {
			logging.Warn(c.logger, "msg", "Error deleting part upload at AbortMultipartUpload", "err", err)
		}
	}

	// Delete part metadata from KV store
	err = c.multiPartStore.deleteAllPartMeta(bucket, key, uploadID)
	if err != nil {
		logging.Warn(c.logger, "msg", "Failed to delete part metadata at AbortMultipartUpload", "err", err)
	}

	// Delete session metadata
	err = c.multiPartStore.deleteUploadMeta(meta)
	if err != nil {
		logging.Warn(c.logger, "msg", "Failed to delete multipart session data at AbortMultipartUpload", "err", err)
		return err
	}

	return nil
}

// ListParts returns the multipart upload session metadata for the given
// bucket/key/uploadID, including uploaded parts with sizes and ETags.
func (c *NatsObjectClient) ListParts(bucket string, key string, uploadID string) (*UploadMeta, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("List parts: [%s/%s], UploadID: %s", bucket, key, uploadID))
	sessionKey := sessionKey(bucket, key, uploadID)
	sessionData, err := c.multiPartStore.getUploadMeta(sessionKey)
	if err != nil {
		return nil, ErrUploadNotFound
	}

	var meta UploadMeta
	if err := json.Unmarshal(sessionData.Value(), &meta); err != nil {
		logging.Error(c.logger, "msg", "Error at ListParts", "err", err)
		return nil, err
	}

	// Populate parts from individual KV entries
	parts, err := c.multiPartStore.getAllPartMeta(bucket, key, uploadID)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at ListParts when getAllPartMeta()", "err", err)
		return nil, err
	}
	meta.Parts = parts

	return &meta, nil
}

// CompleteMultipartUpload concatenates the uploaded parts into the final
// object, computes and returns the multipart ETag, and cleans up temporary
// parts and session metadata.
func (c *NatsObjectClient) CompleteMultipartUpload(bucket string, key string, uploadID string, sortedPartNumbers []int) (string, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Complete multipart upload: [%s/%s], UploadID: %s", bucket, key, uploadID))
	sessionKey := sessionKey(bucket, key, uploadID)
	sessionData, err := c.multiPartStore.getUploadMeta(sessionKey)
	if err != nil {
		return "", ErrUploadNotFound
	}

	var meta UploadMeta
	if err := json.Unmarshal(sessionData.Value(), &meta); err != nil {
		logging.Error(c.logger, "msg", "Error at CompleteMultipartUpload", "err", err)
		return "", err
	}

	// Populate parts from individual KV entries
	parts, err := c.multiPartStore.getAllPartMeta(bucket, key, uploadID)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at CompleteMultipartUpload when getAllPartMeta()", "err", err)
		return "", err
	}
	meta.Parts = parts

	md5Concat := md5.New()
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for _, pn := range sortedPartNumbers {
			pmeta, ok := meta.Parts[pn]
			if !ok {
				_ = pw.CloseWithError(ErrMissingPart)
				return
			}
			partKey := partKey(bucket, key, uploadID, pn)
			rpart, err := c.multiPartStore.getPartUpload(partKey)
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}

			rawHex := strings.Trim(pmeta.ETag, `"`)
			b, _ := hex.DecodeString(rawHex)
			md5Concat.Write(b)

			_, err = io.Copy(pw, rpart)
			rpart.Close()
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}
	}()

	// Ensure the goroutine is cleaned up on early return
	defer func() {
		if err != nil {
			// Close reader to unblock the goroutine if we're returning with an error
			_ = pr.Close()
		}
	}()

	nc := c.client.NATS()
	js, err := nc.JetStream()
	if err != nil {
		logging.Error(c.logger, "msg", "Error at CompleteMultipartUpload", "err", err)
		return "", err
	}
	os, err := js.ObjectStore(bucket)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at CompleteMultipartUpload", "err", err)
		if errors.Is(err, nats.ErrStreamNotFound) {
			return "", ErrBucketNotFound
		}
		return "", err
	}
	_, err = os.Put(&nats.ObjectMeta{Name: key}, pr)
	if err != nil {
		logging.Error(c.logger, "msg", "Error at CompleteMultipartUpload", "err", err)
		return "", err
	}

	etagHex := hex.EncodeToString(md5Concat.Sum(nil))
	finalETag := fmt.Sprintf(`"%s-%d"`, strings.ToLower(etagHex), len(sortedPartNumbers))

	// Delete temporary part data from Object Store
	err = c.multiPartStore.deleteAllPartUpload(bucket, key, uploadID, meta.Parts)
	if err != nil {
		logging.Warn(c.logger, "msg", "Failed to clean multipart temp part data at CompleteMultipartUpload", "err", err)
	}

	// Delete part metadata from KV store
	err = c.multiPartStore.deleteAllPartMeta(bucket, key, uploadID)
	if err != nil {
		logging.Warn(c.logger, "msg", "Failed to clean multipart part metadata at CompleteMultipartUpload", "err", err)
	}

	// Delete session metadata
	err = c.multiPartStore.deleteUploadMeta(meta)
	if err != nil {
		logging.Warn(c.logger, "Failed to delete multipart session data", "err", err)
		return "", err
	}

	return finalETag, nil
}

// sessionKey builds the KV key used to persist state for a multipart upload.
func sessionKey(bucket, key, uploadID string) string {
	return fmt.Sprintf("multi_parts/%s/%s/%s", bucket, key, uploadID)
}

// partKey constructs the Object Store key for a specific uploaded part
func partKey(bucket, key, uploadID string, part int) string {
	return fmt.Sprintf("multi_parts/%s/%s/%s/%06d", bucket, key, uploadID, part)
}

// partMetaKey builds the KV key for a single part's metadata.
func partMetaKey(bucket, key, uploadID string, part int) string {
	return fmt.Sprintf("multi_parts/%s/%s/%s/parts/%06d", bucket, key, uploadID, part)
}

// partMetaPrefix returns the KV key prefix for all parts of an upload session.
func partMetaPrefix(bucket, key, uploadID string) string {
	return fmt.Sprintf("multi_parts/%s/%s/%s/parts/", bucket, key, uploadID)
}
