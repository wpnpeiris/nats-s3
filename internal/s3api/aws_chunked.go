package s3api

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
    sigV4StreamingPayload        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    sigV4StreamingPayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
)

// isAWSSigV4StreamingPayload reports whether the request body uses AWS SigV4 streaming chunks.
func isAWSSigV4StreamingPayload(r *http.Request) bool {
    v := r.Header.Get("x-amz-content-sha256")
    return v == sigV4StreamingPayload || v == sigV4StreamingPayloadTrailer
}

// awsChunkedReader decodes an AWS SigV4 streaming-chunked body, exposing only the decoded payload.
// It ignores the per-chunk signatures and optional trailing headers.
type awsChunkedReader struct {
    br      *bufio.Reader
    remain  int64 // bytes remaining in current chunk; -1 means need to read a new chunk header
    done    bool
    closer  io.Closer
}

// NewAWSChunkedReader wraps an encoded body and returns a reader that yields decoded payload bytes.
func NewAWSChunkedReader(rc io.ReadCloser) io.ReadCloser {
    return &awsChunkedReader{br: bufio.NewReader(rc), remain: -1, closer: rc}
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
    if r.done {
        return 0, io.EOF
    }

    // If no current chunk or previous exhausted, read next chunk header
    if r.remain <= 0 {
        // Read the chunk size line: "<hex-size>;chunk-signature=...\r\n"
        line, err := r.br.ReadString('\n')
        if err != nil {
            return 0, err
        }
        line = strings.TrimRight(line, "\r\n")
        // Parse size before ';' (if any)
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
            // Final chunk. Consume optional trailer headers until an empty line.
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

    // Read payload for current chunk up to requested bytes
    toRead := int64(len(p))
    if toRead > r.remain {
        toRead = r.remain
    }
    n, err := io.ReadFull(r.br, p[:toRead])
    r.remain -= int64(n)
    if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
        return n, err
    }
    // If finished this chunk, consume trailing CRLF and prepare for next chunk
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

func (r *awsChunkedReader) Close() error {
    if r.closer != nil {
        return r.closer.Close()
    }
    return nil
}

