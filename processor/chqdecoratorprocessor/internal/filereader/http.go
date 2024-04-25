package filereader

import (
	"context"
	"io"
	"net/http"
)

type HTTPFileReader struct {
	// URL is the URL of the file to read
	URL    string
	APIKey string

	client *http.Client
}

var _ FileReader = (*HTTPFileReader)(nil)

func NewHTTPFileReader(url string, apikey string, client *http.Client) *HTTPFileReader {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPFileReader{
		URL:    url,
		APIKey: apikey,
		client: client,
	}
}

func (r *HTTPFileReader) ReadFile(ctx context.Context) (data []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
	if err != nil {
		return nil, err
	}
	if r.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+r.APIKey)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (r *HTTPFileReader) Filename() string {
	return r.URL
}
