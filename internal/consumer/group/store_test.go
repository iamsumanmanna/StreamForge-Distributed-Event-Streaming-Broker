package group

import (
	"path/filepath"
	"testing"

	"github.com/distributed-system/streamforge/internal/transport/protocol"
)

func TestDurableOffsets(t *testing.T) {
	dir := t.TempDir()
	c1, err := NewCoordinator(dir, func(string) (int32, error) { return 2, nil })
	if err != nil {
		t.Fatal(err)
	}
	if code := c1.Commit(protocol.CommitOffsetReq{
		GroupID: "g1", Topic: "orders", Partition: 0, Offset: 42,
	}); code != protocol.ErrNone {
		t.Fatalf("commit code %d", code)
	}

	c2, err := NewCoordinator(dir, func(string) (int32, error) { return 2, nil })
	if err != nil {
		t.Fatal(err)
	}
	resp := c2.FetchOffsets("g1", "orders")
	if len(resp.Offsets) != 1 || resp.Offsets[0].Offset != 42 {
		t.Fatalf("got %+v want offset 42 (file %s)", resp, filepath.Join(dir, "groups", "offsets.json"))
	}
}
