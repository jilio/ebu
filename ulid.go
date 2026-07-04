package eventbus

import (
	"crypto/rand"
	"sync"
	"time"
)

// crockford32 is the Crockford base32 alphabet used by ULIDs: no I, L, O, U,
// so identifiers are unambiguous when read or transcribed.
const crockford32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// ulidEntropy buffers crypto/rand reads. Reading 10 bytes per ID directly
// from crypto/rand costs a syscall each time; a buffered reader amortizes it
// across ~100 IDs while keeping the same entropy source.
var ulidEntropy = struct {
	sync.Mutex
	buf []byte
	pos int
}{buf: make([]byte, 1024), pos: 1024}

// NewEventID returns a new ULID: a 26-character, lexicographically sortable,
// globally unique identifier (48-bit millisecond timestamp + 80 bits of
// crypto/rand entropy, Crockford base32).
//
// The bus assigns one to every event it persists (see Event.ID). It is
// exported so custom stores and tests can mint compatible IDs.
//
// IDs sort by creation time to millisecond precision; ties are ordered
// randomly. Uniqueness does not depend on the clock: the 80 random bits alone
// make collisions vanishingly unlikely even at the same millisecond.
func NewEventID() string {
	var bin [16]byte // 6 timestamp bytes + 10 entropy bytes

	ms := uint64(time.Now().UnixMilli())
	bin[0] = byte(ms >> 40)
	bin[1] = byte(ms >> 32)
	bin[2] = byte(ms >> 24)
	bin[3] = byte(ms >> 16)
	bin[4] = byte(ms >> 8)
	bin[5] = byte(ms)

	ulidEntropy.Lock()
	if ulidEntropy.pos+10 > len(ulidEntropy.buf) {
		// crypto/rand.Read never fails on supported platforms (it panics
		// internally on the truly unrecoverable ones), so the error is
		// impossible to surface meaningfully here.
		rand.Read(ulidEntropy.buf)
		ulidEntropy.pos = 0
	}
	copy(bin[6:], ulidEntropy.buf[ulidEntropy.pos:ulidEntropy.pos+10])
	ulidEntropy.pos += 10
	ulidEntropy.Unlock()

	// 128 bits -> 26 base32 characters (the top bit pair of the first
	// character is always zero: 26*5 = 130 bits of space for 128 bits).
	var out [26]byte
	out[0] = crockford32[bin[0]>>5]
	bitPos := 3 // bits already consumed from bin
	for i := 1; i < 26; i++ {
		byteIdx := bitPos / 8
		shift := bitPos % 8
		v := bin[byteIdx] << shift >> 3
		if shift > 3 && byteIdx+1 < len(bin) {
			v |= bin[byteIdx+1] >> (11 - shift)
		}
		out[i] = crockford32[v&0x1f]
		bitPos += 5
	}
	return string(out[:])
}
