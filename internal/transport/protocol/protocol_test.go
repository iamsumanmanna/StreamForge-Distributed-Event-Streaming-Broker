package protocol

import (
	"bytes"
	"testing"
)

func TestRecordDiskRoundTrip(t *testing.T) {
	raw := EncodeRecordDisk(12345, 2, []byte("key"), []byte("value"))
	ts, attrs, key, val, n, err := DecodeRecordDisk(raw)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(raw) || ts != 12345 || attrs != 2 || string(key) != "key" || string(val) != "value" {
		t.Fatalf("mismatch n=%d ts=%d attrs=%d key=%s val=%s", n, ts, attrs, key, val)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	body := EncodeRequest(APIProduce, 1, 42, EncodeProduceReq(ProduceReq{
		Topic: "t", Partition: 1, Key: []byte("k"), Value: []byte("v"),
		Acks: AcksAll, Compression: 1,
	}))
	if err := EncodeFrame(&buf, body); err != nil {
		t.Fatal(err)
	}
	frame, err := DecodeFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	h, payload, err := DecodeRequest(frame)
	if err != nil || h.CorrelationID != 42 || h.APIKey != APIProduce {
		t.Fatalf("header %+v err=%v", h, err)
	}
	req, err := DecodeProduceReq(payload)
	if err != nil || req.Topic != "t" || string(req.Value) != "v" || req.Acks != AcksAll || req.Compression != 1 {
		t.Fatalf("req %+v err=%v", req, err)
	}
}
