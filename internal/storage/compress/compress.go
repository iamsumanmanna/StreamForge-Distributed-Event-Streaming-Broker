package compress

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Codecs stored in record attrs (low 3 bits).
const (
	None   uint8 = 0
	Gzip   uint8 = 1
	Snappy uint8 = 2
	LZ4    uint8 = 3
	Zstd   uint8 = 4
)

func Parse(name string) (uint8, error) {
	switch name {
	case "", "none", "uncompressed":
		return None, nil
	case "gzip":
		return Gzip, nil
	case "snappy":
		return Snappy, nil
	case "lz4":
		return LZ4, nil
	case "zstd":
		return Zstd, nil
	default:
		return 0, fmt.Errorf("unknown compression %q", name)
	}
}

func Name(c uint8) string {
	switch c & 0x07 {
	case Gzip:
		return "gzip"
	case Snappy:
		return "snappy"
	case LZ4:
		return "lz4"
	case Zstd:
		return "zstd"
	default:
		return "none"
	}
}

func Encode(codec uint8, plain []byte) ([]byte, error) {
	switch codec & 0x07 {
	case None:
		return plain, nil
	case Gzip:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(plain); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case Snappy:
		return snappy.Encode(nil, plain), nil
	case LZ4:
		var buf bytes.Buffer
		w := lz4.NewWriter(&buf)
		if _, err := w.Write(plain); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case Zstd:
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			return nil, err
		}
		defer enc.Close()
		return enc.EncodeAll(plain, nil), nil
	default:
		return nil, fmt.Errorf("unsupported codec %d", codec)
	}
}

func Decode(codec uint8, data []byte) ([]byte, error) {
	switch codec & 0x07 {
	case None:
		return data, nil
	case Gzip:
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	case Snappy:
		return snappy.Decode(nil, data)
	case LZ4:
		r := lz4.NewReader(bytes.NewReader(data))
		return io.ReadAll(r)
	case Zstd:
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		defer dec.Close()
		return dec.DecodeAll(data, nil)
	default:
		return nil, fmt.Errorf("unsupported codec %d", codec)
	}
}
