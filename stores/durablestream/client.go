package durablestream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Client handles HTTP communication with a durable-streams server.
type Client struct {
	baseURL    string
	httpClient *http.Client
	cfg        *config
}

func newClient(baseURL string, cfg *config) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: cfg.httpClient,
		cfg:        cfg,
	}
}

// Response wraps an HTTP response with durable-streams specific data.
type Response struct {
	StatusCode int
	NextOffset string
	Body       []byte
	UpToDate   bool
}

// Create creates a new stream at the given URL.
func (c *Client) Create(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", c.cfg.contentType)

	resp, err := c.doWithRetry(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create stream: status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Append adds data to the stream and returns the next offset.
func (c *Client) Append(ctx context.Context, data []byte) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", c.cfg.contentType)

	resp, err := c.doWithRetry(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("append: status %d: %s", resp.StatusCode, string(body))
	}

	return resp.Header.Get("Stream-Next-Offset"), nil
}

// Read fetches data from the stream starting at the given offset.
func (c *Client) Read(ctx context.Context, offset string, limit int) (*Response, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse URL: %w", err)
	}
	return c.readWithURL(ctx, u, offset, limit)
}

// readWithURL is the internal implementation that accepts a pre-parsed URL.
// This allows testing error paths that are unreachable through the public API.
func (c *Client) readWithURL(ctx context.Context, u *url.URL, offset string, limit int) (*Response, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.timeout)
	defer cancel()

	q := u.Query()
	q.Set("offset", offset)
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("stream not found")
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("read: status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return &Response{
		StatusCode: resp.StatusCode,
		NextOffset: resp.Header.Get("Stream-Next-Offset"),
		Body:       body,
		UpToDate:   resp.Header.Get("Stream-Up-To-Date") == "true",
	}, nil
}

// doWithRetry executes the request with retry logic.
func (c *Client) doWithRetry(req *http.Request) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= c.cfg.retryAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(c.cfg.retryBackoff * time.Duration(attempt)):
			}
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		// Retry on 5xx errors
		if resp.StatusCode >= 500 && attempt < c.cfg.retryAttempts {
			resp.Body.Close()
			lastErr = fmt.Errorf("server error: %d", resp.StatusCode)
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("after %d retries: %w", c.cfg.retryAttempts, lastErr)
}
