package metrics

import (
	"context"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()
	router := mux.NewRouter()
	RegisterMetricEndpoint(router)
	ts := httptest.NewServer(router)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Errorf("Expected to be response 200 OK")
	}

	if !strings.Contains(res.Header.Get("Content-type"), "text/plain") {
		t.Errorf("Expected to be response content type 'text/plain'")
	}

	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Errorf("Expected to be no errors when reading the body %v", err)
	}

	if data == nil {
		t.Errorf("Expected to be nonempty response")
	}
}
