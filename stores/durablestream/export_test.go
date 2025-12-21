package durablestream

import (
	"context"
	"net/url"
)

// ReadWithURL exposes readWithURL for testing error paths that are
// unreachable through the public API (e.g., NewRequestWithContext errors).
func (c *Client) ReadWithURL(ctx context.Context, u *url.URL, offset string, limit int) (*Response, error) {
	return c.readWithURL(ctx, u, offset, limit)
}

// NewClientForTest creates a client for testing purposes.
func NewClientForTest(baseURL string, cfg *config) *Client {
	return newClient(baseURL, cfg)
}
