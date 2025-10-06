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
}

// MultiPartStore groups storage backends used for multipart uploads.
// sessionStore tracks session metadata in a Key-Value bucket, while
// tempPartStore holds uploaded parts in a temporary Object Store.
type MultiPartStore struct {
	logger        log.Logger
	sessionStore  nats.KeyValue
	tempPartStore nats.ObjectStore
}

func newMultiPartStore(logger log.Logger, c *Client) *MultiPartStore {
	nc := c.NATS()
	js, err := nc.JetStream()
	if err != nil {
		panic("Failed to create to multipart session store when nc.JetStream()")
	}

	kv, err := js.KeyValue(MultiPartSessionStoreName)
	if err != nil {
		if errors.Is(err, nats.ErrBucketNotFound) {
			kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket: MultiPartSessionStoreName,
			})
			if err != nil {
				panic("Failed to create to multipart session store when js.CreateKeyValue()")
			}
		} else {
			panic("Failed to create to multipart session store when js.KeyValue()")
		}
	}

	os, err := js.ObjectStore(MultiPartTempStoreName)
	if err != nil && errors.Is(err, nats.ErrStreamNotFound) {
		if errors.Is(err, nats.ErrStreamNotFound) {
			os, err = js.CreateObjectStore(&nats.ObjectStoreConfig{
				Bucket: MultiPartTempStoreName,
			})
			if err != nil {
				panic("Failed to create to multipart temp store when js.CreateObjectStore()")
			}
		} else {
			panic("Failed to create to multipart temp store when js.ObjectStore()")
		}
	}

	return &MultiPartStore{
		logger:        logger,
		sessionStore:  kv,
		tempPartStore: os,
	}
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
	logging.Debug(m.logger, "msg", fmt.Sprintf("creating part upload: %s", partKey))
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

// deletePartUpload delete part from the temporary Object Store.
func (m *MultiPartStore) deletePartUpload(partKey string) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("delete part upload: %s", partKey))
	return m.tempPartStore.Delete(partKey)
}

// deleteAllPartUpload delete all parts from the temporary Object Store.
func (m *MultiPartStore) deleteAllPartUpload(meta UploadMeta) error {
	logging.Debug(m.logger, "msg", fmt.Sprintf("delete all part upload: %v", meta))
	for pn := range meta.Parts {
		key := partKey(meta.Bucket, meta.Key, meta.UploadID, pn)
		err := m.tempPartStore.Delete(key)
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

func NewNatsObjectClient(logger log.Logger, natsClient *Client) *NatsObjectClient {
	mps := newMultiPartStore(logger, natsClient)
	return &NatsObjectClient{
		logger:         logger,
		client:         natsClient,
		multiPartStore: mps,
	}
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
		Parts:     map[int]PartMeta{},
	}

	return c.multiPartStore.createUploadMeta(meta)
}

// UploadPart streams a part into temporary storage and records its ETag/size
// under the multipart session. Returns the hex ETag (without quotes).
func (c *NatsObjectClient) UploadPart(bucket string, key string, uploadID string, part int, dataReader io.ReadCloser) (string, error) {
	logging.Info(c.logger, "msg", fmt.Sprintf("Upload part: [%s/%s], UploadID: %s", bucket, key, uploadID))
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
		return "", err
	}

	etag := strings.ToLower(hex.EncodeToString(h.Sum(nil)))
	meta.Parts[part] = PartMeta{
		Number: part, ETag: `"` + etag + `"`, Size: obj.Size, StoredAt: time.Now().Unix(),
	}

	err = c.multiPartStore.saveUploadMeta(meta, sessionData.Revision())
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

	for pn := range meta.Parts {
		partKey := partKey(bucket, key, uploadID, pn)
		err := c.multiPartStore.deletePartUpload(partKey)
		if err != nil {
			logging.Error(c.logger, "msg", "Error at AbortMultipartUpload", "err", err)
		}
	}

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

	err = c.multiPartStore.deleteAllPartUpload(meta)
	if err != nil {
		logging.Warn(c.logger, "msg", "Failed to clean multipart session/temp data at CompleteMultipartUpload", "err", err)
	}

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
