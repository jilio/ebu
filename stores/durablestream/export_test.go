package durablestream

// SetStreamURLForTest replaces the URL producer appends are posted to, so a
// test can exercise request-construction failures that a URL accepted by New
// cannot produce.
func SetStreamURLForTest(s *Store, url string) {
	s.streamURL = url
}
