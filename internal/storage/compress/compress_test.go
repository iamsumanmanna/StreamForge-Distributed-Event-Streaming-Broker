package compress

import "testing"

func TestRoundTrip(t *testing.T) {
	plain := []byte("hello streamforge compression payload xxxxxxxxx")
	for _, codec := range []uint8{None, Gzip, Snappy, LZ4, Zstd} {
		enc, err := Encode(codec, plain)
		if err != nil {
			t.Fatalf("encode %s: %v", Name(codec), err)
		}
		dec, err := Decode(codec, enc)
		if err != nil {
			t.Fatalf("decode %s: %v", Name(codec), err)
		}
		if string(dec) != string(plain) {
			t.Fatalf("mismatch codec %s", Name(codec))
		}
	}
}
