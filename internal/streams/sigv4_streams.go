package streams

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Constants used by SigV4 streaming payloads
const (
	SigV4StreamingPayload        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	SigV4StreamingPayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
)

// IsSigV4StreamingPayload reports whether the request body uses SigV4 streaming chunks.
func IsSigV4StreamingPayload(r *http.Request) bool {
	v := r.Header.Get("x-amz-content-sha256")
	return v == SigV4StreamingPayload || v == SigV4StreamingPayloadTrailer
}

// SigV4StreamReader decodes a SigV4 streaming-chunked body, exposing only the decoded payload.
type SigV4StreamReader struct {
	br     *bufio.Reader
	remain int64 // bytes remaining in current chunk; -1 means need to read a new chunk header
	done   bool
	closer io.Closer
}

// NewSigV4StreamReader wraps an encoded body and returns a reader that yields decoded payload bytes.
func NewSigV4StreamReader(rc io.ReadCloser) io.ReadCloser {
	return &SigV4StreamReader{br: bufio.NewReader(rc), remain: -1, closer: rc}
}

func (r *SigV4StreamReader) Read(p []byte) (int, error) {
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
			return 0, fmt.Errorf("invalid sigv4-chunked size line: empty")
		}
		sz, err := strconv.ParseInt(sizeStr, 16, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid sigv4-chunked size: %w", err)
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

func (r *SigV4StreamReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

// DecodedContentLength returns the parsed value of x-amz-decoded-content-length, if present and valid.
func DecodedContentLength(r *http.Request) (int64, bool) {
	v := r.Header.Get("x-amz-decoded-content-length")
	if v == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

var (
	ErrDecodedLengthExceedsLimit = errors.New("decoded length exceeds limit")
	ErrDecodedLengthMismatch     = errors.New("decoded length mismatch")
)

// CheckDecodedLengthLimit returns ErrDecodedLengthExceedsLimit if the decoded length header
// is present and exceeds the provided limit.
func CheckDecodedLengthLimit(r *http.Request, limit int64) error {
	if dl, ok := DecodedContentLength(r); ok && dl > limit {
		return ErrDecodedLengthExceedsLimit
	}
	return nil
}

// CheckDecodedLengthMatches returns ErrDecodedLengthMismatch if the decoded length header
// is present and does not match the provided actual length.
func CheckDecodedLengthMatches(r *http.Request, actual int64) error {
	if dl, ok := DecodedContentLength(r); ok && dl != actual {
		return ErrDecodedLengthMismatch
	}
	return nil
}

// NewLimitedSigV4StreamReader returns an io.ReadCloser that decodes the SigV4 stream and enforces a byte limit.
func NewLimitedSigV4StreamReader(rc io.ReadCloser, limit int64) io.ReadCloser {
	dec := NewSigV4StreamReader(rc)
	return &limitedRC{Reader: io.LimitReader(dec, limit), Closer: dec}
}

type limitedRC struct {
	Reader io.Reader
	Closer io.Closer
}

func (l *limitedRC) Read(p []byte) (int, error) { return l.Reader.Read(p) }
func (l *limitedRC) Close() error {
	if l.Closer != nil {
		return l.Closer.Close()
	}
	return nil
}
