package streams

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Constants used by AWS SigV4 streaming payloads
const (
	SigV4StreamingPayload        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	SigV4StreamingPayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
)

// IsAWSSigV4StreamingPayload reports whether the request body uses AWS SigV4 streaming chunks.
func IsAWSSigV4StreamingPayload(r *http.Request) bool {
	v := r.Header.Get("x-amz-content-sha256")
	return v == SigV4StreamingPayload || v == SigV4StreamingPayloadTrailer
}

// awsStreamReader decodes an AWS SigV4 streaming-chunked body, exposing only the decoded payload.
type awsStreamReader struct {
	br     *bufio.Reader
	remain int64 // bytes remaining in current chunk; -1 means need to read a new chunk header
	done   bool
	closer io.Closer
}

// NewAWSStreamReader wraps an encoded body and returns a reader that yields decoded payload bytes.
func NewAWSStreamReader(rc io.ReadCloser) io.ReadCloser {
	return &awsStreamReader{br: bufio.NewReader(rc), remain: -1, closer: rc}
}

func (r *awsStreamReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}

	if r.remain <= 0 {
		line, err := r.br.ReadString('\n')
		if err != nil {
			return 0, err
		}
		line = strings.TrimRight(line, "\r\n")
		sizeStr := line
		if i := strings.IndexByte(line, ';'); i >= 0 {
			sizeStr = line[:i]
		}
		sizeStr = strings.TrimSpace(sizeStr)
		if sizeStr == "" {
			return 0, fmt.Errorf("invalid aws-chunked size line: empty")
		}
		sz, err := strconv.ParseInt(sizeStr, 16, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid aws-chunked size: %w", err)
		}
		if sz == 0 {
			for {
				l, err := r.br.ReadString('\n')
				if err != nil {
					return 0, err
				}
				if l == "\r\n" || l == "\n" {
					break
				}
			}
			r.done = true
			return 0, io.EOF
		}
		r.remain = sz
	}

	toRead := int64(len(p))
	if toRead > r.remain {
		toRead = r.remain
	}
	n, err := io.ReadFull(r.br, p[:toRead])
	r.remain -= int64(n)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return n, err
	}
	if r.remain == 0 {
		if _, err := r.br.ReadByte(); err != nil { // '\r'
			return n, err
		}
		if _, err := r.br.ReadByte(); err != nil { // '\n'
			return n, err
		}
		r.remain = -1
	}
	if n == 0 && !r.done {
		return 0, io.EOF
	}
	return n, nil
}

func (r *awsStreamReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}
