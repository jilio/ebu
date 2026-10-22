package sqlite

import (
	"fmt"
	"strings"
	"time"
)

// storedTime scans an events.timestamp value. The driver writes time.Time
// parameters as text and converts that text back to time.Time only for the
// formats it recognizes; a timestamp whose zone has a numeric or
// non-alphabetic name comes back as a string. That covers fixed zones such as
// "+0500" (what time.Parse produces for an RFC 3339 offset outside the local
// zone) and local zones whose abbreviation is an offset (Asia/Kathmandu
// "+0545", Asia/Colombo, Asia/Tehran, ...), including time.Now values with
// their monotonic suffix. Those strings are Go's time.Time.String form, and
// the instant they denote is fully determined by the date, the clock and the
// numeric offset.
type storedTime struct {
	time.Time
}

// Scan implements sql.Scanner.
func (t *storedTime) Scan(value any) error {
	switch v := value.(type) {
	case time.Time:
		t.Time = v
		return nil
	case string:
		return t.parse(v)
	case []byte:
		return t.parse(string(v))
	case nil:
		t.Time = time.Time{}
		return nil
	default:
		return fmt.Errorf("sqlite: unsupported timestamp type %T", value)
	}
}

// storedTimeLayouts are tried in order; the first matches time.Time.String
// without its zone name, which only labels the numeric offset.
var storedTimeLayouts = []string{
	"2006-01-02 15:04:05.999999999 -0700",
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999",
}

func (t *storedTime) parse(text string) error {
	// time.Time.String appends the monotonic clock reading (" m=+0.012")
	// for values from time.Now, such as the bus's own timestamps; it has no
	// bearing on the instant.
	if i := strings.Index(text, " m="); i >= 0 {
		text = text[:i]
	}
	// Drop a trailing zone name ("UTC", "+0500", "UTC+5") after the offset.
	if fields := strings.Fields(text); len(fields) == 4 {
		text = strings.Join(fields[:3], " ")
	}
	for _, layout := range storedTimeLayouts {
		if parsed, err := time.Parse(layout, text); err == nil {
			t.Time = parsed
			return nil
		}
	}
	return fmt.Errorf("sqlite: unparsable timestamp %q", text)
}
