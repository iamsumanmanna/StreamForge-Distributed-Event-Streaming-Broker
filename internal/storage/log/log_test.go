package log

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestAppendReadRecover(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.SegmentMaxBytes = 1 << 20
	opts.FsyncEveryN = 1
	opts.FsyncInterval = time.Hour

	p, err := OpenPartition(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		val := []byte(fmt.Sprintf("v%d", i))
		off, err := p.Append(key, val)
		if err != nil {
			t.Fatal(err)
		}
		if off != int64(i) {
			t.Fatalf("offset %d want %d", off, i)
		}
	}
	recs, hwm, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if hwm != n {
		t.Fatalf("hwm %d", hwm)
	}
	if len(recs) != n {
		t.Fatalf("got %d records", len(recs))
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p2, err := OpenPartition(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	recs, _, err = p2.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != n {
		t.Fatalf("after recover got %d", len(recs))
	}
	for i, r := range recs {
		if string(r.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("value mismatch at %d", i)
		}
	}
}

func TestSegmentRoll(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.SegmentMaxBytes = 512
	opts.FsyncInterval = time.Hour
	p, err := OpenPartition(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := 0; i < 100; i++ {
		_, err := p.Append([]byte("key"), []byte(fmt.Sprintf("value-%d-padding-xxxxxxxx", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(p.segments) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(p.segments))
	}
	recs, _, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 100 {
		t.Fatalf("got %d", len(recs))
	}
}

func TestTopicManager(t *testing.T) {
	root := t.TempDir()
	tm, err := NewTopicManager(root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := tm.CreateTopic("orders", 3); err != nil {
		t.Fatal(err)
	}
	part, err := tm.Get("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	off, err := part.Append([]byte("a"), []byte("b"))
	if err != nil || off != 0 {
		t.Fatalf("off=%d err=%v", off, err)
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
	tm2, err := NewTopicManager(root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer tm2.Close()
	part, err = tm2.Get("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	recs, _, err := part.Read(0, 1024)
	if err != nil || len(recs) != 1 {
		t.Fatalf("recs=%d err=%v", len(recs), err)
	}
	_ = filepath.Join(root, "orders")
}
