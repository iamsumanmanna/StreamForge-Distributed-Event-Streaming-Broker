package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// API keys — keep stable across versions.
const (
	APIProduce       uint16 = 1
	APIFetch         uint16 = 2
	APICreateTopic   uint16 = 3
	APIMetadata      uint16 = 4
	APIJoinGroup     uint16 = 5
	APISyncGroup     uint16 = 6
	APIHeartbeat     uint16 = 7
	APILeaveGroup    uint16 = 8
	APICommitOffset  uint16 = 9
	APIFetchOffsets  uint16 = 10
	APIReplicaFetch  uint16 = 11
	APIReplicaAppend uint16 = 12
	APIAuth          uint16 = 13
)

// Producer acks (Kafka-like).
const (
	AcksNone   int16 = 0  // fire-and-forget (no durability wait)
	AcksLeader int16 = 1  // wait for local append (+ optional fsync)
	AcksAll    int16 = -1 // wait for majority replica ack
)

// Error codes.
const (
	ErrNone             uint16 = 0
	ErrUnknown          uint16 = 1
	ErrTopicExists      uint16 = 2
	ErrUnknownTopic     uint16 = 3
	ErrUnknownPartition uint16 = 4
	ErrInvalidRequest   uint16 = 5
	ErrNotLeader        uint16 = 6
	ErrOffsetOutOfRange uint16 = 7
	ErrGroupCoordinator uint16 = 8
	ErrRebalanceInProg  uint16 = 9
	ErrUnknownMember    uint16 = 10
	ErrUnauthorized     uint16 = 11
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Header is common request framing after frame length.
type Header struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
}

// Record is an in-memory message.
type Record struct {
	Offset    int64
	Timestamp int64
	Attrs     uint8 // low 3 bits = compression codec
	Key       []byte
	Value     []byte
}

// EncodeFrame writes a length-prefixed frame: [len][body...]
func EncodeFrame(w io.Writer, body []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(body)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

// DecodeFrame reads one length-prefixed frame.
func DecodeFrame(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > 32<<20 { // 32 MiB cap
		return nil, fmt.Errorf("frame too large: %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func EncodeRequest(apiKey, apiVersion uint16, corr uint32, payload []byte) []byte {
	buf := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint16(buf[0:2], apiKey)
	binary.BigEndian.PutUint16(buf[2:4], apiVersion)
	binary.BigEndian.PutUint32(buf[4:8], corr)
	copy(buf[8:], payload)
	return buf
}

func DecodeRequest(body []byte) (Header, []byte, error) {
	if len(body) < 8 {
		return Header{}, nil, errors.New("request too short")
	}
	h := Header{
		APIKey:        binary.BigEndian.Uint16(body[0:2]),
		APIVersion:    binary.BigEndian.Uint16(body[2:4]),
		CorrelationID: binary.BigEndian.Uint32(body[4:8]),
	}
	return h, body[8:], nil
}

func EncodeResponse(corr uint32, errCode uint16, payload []byte) []byte {
	buf := make([]byte, 6+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], corr)
	binary.BigEndian.PutUint16(buf[4:6], errCode)
	copy(buf[6:], payload)
	return buf
}

func DecodeResponse(body []byte) (corr uint32, errCode uint16, payload []byte, err error) {
	if len(body) < 6 {
		return 0, 0, nil, errors.New("response too short")
	}
	corr = binary.BigEndian.Uint32(body[0:4])
	errCode = binary.BigEndian.Uint16(body[4:6])
	payload = body[6:]
	return
}

// --- helpers for strings / bytes ---

func PutString(buf []byte, s string) []byte {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(len(s)))
	buf = append(buf, tmp[:]...)
	return append(buf, s...)
}

func GetString(buf []byte) (string, []byte, error) {
	if len(buf) < 2 {
		return "", nil, errors.New("short string")
	}
	n := int(binary.BigEndian.Uint16(buf[0:2]))
	buf = buf[2:]
	if len(buf) < n {
		return "", nil, errors.New("short string body")
	}
	return string(buf[:n]), buf[n:], nil
}

func PutBytes(buf, b []byte) []byte {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(len(b)))
	buf = append(buf, tmp[:]...)
	return append(buf, b...)
}

func GetBytes(buf []byte) ([]byte, []byte, error) {
	if len(buf) < 4 {
		return nil, nil, errors.New("short bytes")
	}
	n := int(binary.BigEndian.Uint32(buf[0:4]))
	buf = buf[4:]
	if len(buf) < n {
		return nil, nil, errors.New("short bytes body")
	}
	out := make([]byte, n)
	copy(out, buf[:n])
	return out, buf[n:], nil
}

func PutInt32(buf []byte, v int32) []byte {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	return append(buf, tmp[:]...)
}

func PutInt64(buf []byte, v int64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	return append(buf, tmp[:]...)
}

func GetInt32(buf []byte) (int32, []byte, error) {
	if len(buf) < 4 {
		return 0, nil, errors.New("short int32")
	}
	v := int32(binary.BigEndian.Uint32(buf[0:4]))
	return v, buf[4:], nil
}

func GetInt64(buf []byte) (int64, []byte, error) {
	if len(buf) < 8 {
		return 0, nil, errors.New("short int64")
	}
	v := int64(binary.BigEndian.Uint64(buf[0:8]))
	return v, buf[8:], nil
}

func PutUint16(buf []byte, v uint16) []byte {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], v)
	return append(buf, tmp[:]...)
}

func GetUint16(buf []byte) (uint16, []byte, error) {
	if len(buf) < 2 {
		return 0, nil, errors.New("short uint16")
	}
	return binary.BigEndian.Uint16(buf[0:2]), buf[2:], nil
}

// EncodeRecordDisk encodes a record for the log file (without offset; offset is positional).
// Format: [length u32][crc u32][timestamp u64][attrs u8][keyLen u32][key][valLen u32][val]
func EncodeRecordDisk(ts int64, attrs uint8, key, value []byte) []byte {
	payloadLen := 8 + 1 + 4 + len(key) + 4 + len(value)
	buf := make([]byte, 8+payloadLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen+4)) // length includes crc
	body := buf[8:]
	binary.BigEndian.PutUint64(body[0:8], uint64(ts))
	body[8] = attrs
	binary.BigEndian.PutUint32(body[9:13], uint32(len(key)))
	copy(body[13:], key)
	off := 13 + len(key)
	binary.BigEndian.PutUint32(body[off:off+4], uint32(len(value)))
	copy(body[off+4:], value)
	crc := crc32.Checksum(body, crcTable)
	binary.BigEndian.PutUint32(buf[4:8], crc)
	return buf
}

// DecodeRecordDisk reads one record starting at buf; returns record fields, bytes consumed.
func DecodeRecordDisk(buf []byte) (ts int64, attrs uint8, key, value []byte, n int, err error) {
	if len(buf) < 8 {
		return 0, 0, nil, nil, 0, io.ErrUnexpectedEOF
	}
	total := int(binary.BigEndian.Uint32(buf[0:4]))
	if total < 4 || len(buf) < 4+total {
		return 0, 0, nil, nil, 0, io.ErrUnexpectedEOF
	}
	crcWant := binary.BigEndian.Uint32(buf[4:8])
	body := buf[8 : 4+total]
	if crc32.Checksum(body, crcTable) != crcWant {
		return 0, 0, nil, nil, 0, errors.New("crc mismatch")
	}
	if len(body) < 8+1+4 {
		return 0, 0, nil, nil, 0, errors.New("short record")
	}
	ts = int64(binary.BigEndian.Uint64(body[0:8]))
	attrs = body[8]
	keyLen := int(binary.BigEndian.Uint32(body[9:13]))
	rest := body[13:]
	if len(rest) < keyLen+4 {
		return 0, 0, nil, nil, 0, errors.New("short key")
	}
	key = make([]byte, keyLen)
	copy(key, rest[:keyLen])
	rest = rest[keyLen:]
	valLen := int(binary.BigEndian.Uint32(rest[0:4]))
	rest = rest[4:]
	if len(rest) < valLen {
		return 0, 0, nil, nil, 0, errors.New("short value")
	}
	value = make([]byte, valLen)
	copy(value, rest[:valLen])
	return ts, attrs, key, value, 4 + total, nil
}

// Produce request/response codecs

type ProduceReq struct {
	Topic       string
	Partition   int32
	Key         []byte
	Value       []byte
	Acks        int16 // 0, 1, or -1 (all)
	Compression uint8 // see compress package codecs
}

func EncodeProduceReq(r ProduceReq) []byte {
	var b []byte
	b = PutString(b, r.Topic)
	b = PutInt32(b, r.Partition)
	b = PutBytes(b, r.Key)
	b = PutBytes(b, r.Value)
	b = PutInt16(b, r.Acks)
	b = append(b, r.Compression)
	return b
}

func DecodeProduceReq(buf []byte) (ProduceReq, error) {
	var r ProduceReq
	var err error
	r.Topic, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.Partition, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	r.Key, buf, err = GetBytes(buf)
	if err != nil {
		return r, err
	}
	r.Value, buf, err = GetBytes(buf)
	if err != nil {
		return r, err
	}
	r.Acks = AcksLeader
	if len(buf) >= 2 {
		r.Acks, buf, err = GetInt16(buf)
		if err != nil {
			return r, err
		}
	}
	if len(buf) >= 1 {
		r.Compression = buf[0]
	}
	return r, nil
}

func PutInt16(buf []byte, v int16) []byte {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	return append(buf, tmp[:]...)
}

func GetInt16(buf []byte) (int16, []byte, error) {
	if len(buf) < 2 {
		return 0, nil, errors.New("short int16")
	}
	return int16(binary.BigEndian.Uint16(buf[0:2])), buf[2:], nil
}

type AuthReq struct {
	Token string
}

func EncodeAuthReq(r AuthReq) []byte {
	return PutString(nil, r.Token)
}

func DecodeAuthReq(buf []byte) (AuthReq, error) {
	tok, _, err := GetString(buf)
	return AuthReq{Token: tok}, err
}

type ProduceResp struct {
	Offset int64
}

func EncodeProduceResp(r ProduceResp) []byte {
	return PutInt64(nil, r.Offset)
}

func DecodeProduceResp(buf []byte) (ProduceResp, error) {
	off, _, err := GetInt64(buf)
	return ProduceResp{Offset: off}, err
}

type FetchReq struct {
	Topic     string
	Partition int32
	Offset    int64
	MaxBytes  int32
}

func EncodeFetchReq(r FetchReq) []byte {
	var b []byte
	b = PutString(b, r.Topic)
	b = PutInt32(b, r.Partition)
	b = PutInt64(b, r.Offset)
	b = PutInt32(b, r.MaxBytes)
	return b
}

func DecodeFetchReq(buf []byte) (FetchReq, error) {
	var r FetchReq
	var err error
	r.Topic, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.Partition, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	r.Offset, buf, err = GetInt64(buf)
	if err != nil {
		return r, err
	}
	r.MaxBytes, _, err = GetInt32(buf)
	return r, err
}

type FetchResp struct {
	HighWatermark int64
	Records       []Record
}

func EncodeFetchResp(r FetchResp) []byte {
	var b []byte
	b = PutInt64(b, r.HighWatermark)
	b = PutInt32(b, int32(len(r.Records)))
	for _, rec := range r.Records {
		b = PutInt64(b, rec.Offset)
		b = PutInt64(b, rec.Timestamp)
		b = append(b, rec.Attrs)
		b = PutBytes(b, rec.Key)
		b = PutBytes(b, rec.Value)
	}
	return b
}

func DecodeFetchResp(buf []byte) (FetchResp, error) {
	var r FetchResp
	var err error
	r.HighWatermark, buf, err = GetInt64(buf)
	if err != nil {
		return r, err
	}
	var n int32
	n, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	r.Records = make([]Record, 0, n)
	for i := int32(0); i < n; i++ {
		var rec Record
		rec.Offset, buf, err = GetInt64(buf)
		if err != nil {
			return r, err
		}
		rec.Timestamp, buf, err = GetInt64(buf)
		if err != nil {
			return r, err
		}
		if len(buf) < 1 {
			return r, errors.New("short attrs")
		}
		rec.Attrs = buf[0]
		buf = buf[1:]
		rec.Key, buf, err = GetBytes(buf)
		if err != nil {
			return r, err
		}
		rec.Value, buf, err = GetBytes(buf)
		if err != nil {
			return r, err
		}
		r.Records = append(r.Records, rec)
	}
	return r, nil
}

type CreateTopicReq struct {
	Topic      string
	Partitions int32
}

func EncodeCreateTopicReq(r CreateTopicReq) []byte {
	var b []byte
	b = PutString(b, r.Topic)
	b = PutInt32(b, r.Partitions)
	return b
}

func DecodeCreateTopicReq(buf []byte) (CreateTopicReq, error) {
	var r CreateTopicReq
	var err error
	r.Topic, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.Partitions, _, err = GetInt32(buf)
	return r, err
}

type PartitionMeta struct {
	ID     int32
	Leader string
}

type TopicMeta struct {
	Name       string
	Partitions []PartitionMeta
}

type MetadataResp struct {
	NodeID string
	Topics []TopicMeta
}

func EncodeMetadataResp(r MetadataResp) []byte {
	var b []byte
	b = PutString(b, r.NodeID)
	b = PutInt32(b, int32(len(r.Topics)))
	for _, t := range r.Topics {
		b = PutString(b, t.Name)
		b = PutInt32(b, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			b = PutInt32(b, p.ID)
			b = PutString(b, p.Leader)
		}
	}
	return b
}

func DecodeMetadataResp(buf []byte) (MetadataResp, error) {
	var r MetadataResp
	var err error
	r.NodeID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	var nt int32
	nt, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	for i := int32(0); i < nt; i++ {
		var t TopicMeta
		t.Name, buf, err = GetString(buf)
		if err != nil {
			return r, err
		}
		var np int32
		np, buf, err = GetInt32(buf)
		if err != nil {
			return r, err
		}
		for j := int32(0); j < np; j++ {
			var p PartitionMeta
			p.ID, buf, err = GetInt32(buf)
			if err != nil {
				return r, err
			}
			p.Leader, buf, err = GetString(buf)
			if err != nil {
				return r, err
			}
			t.Partitions = append(t.Partitions, p)
		}
		r.Topics = append(r.Topics, t)
	}
	return r, nil
}

// Group protocol messages

type JoinGroupReq struct {
	GroupID  string
	MemberID string
	Topics   []string
}

func EncodeJoinGroupReq(r JoinGroupReq) []byte {
	var b []byte
	b = PutString(b, r.GroupID)
	b = PutString(b, r.MemberID)
	b = PutInt32(b, int32(len(r.Topics)))
	for _, t := range r.Topics {
		b = PutString(b, t)
	}
	return b
}

func DecodeJoinGroupReq(buf []byte) (JoinGroupReq, error) {
	var r JoinGroupReq
	var err error
	r.GroupID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.MemberID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	var n int32
	n, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	for i := int32(0); i < n; i++ {
		var t string
		t, buf, err = GetString(buf)
		if err != nil {
			return r, err
		}
		r.Topics = append(r.Topics, t)
	}
	return r, nil
}

type Assignment struct {
	Topic     string
	Partition int32
}

type JoinGroupResp struct {
	MemberID     string
	GenerationID int32
	LeaderID     string
	Members      []string
}

func EncodeJoinGroupResp(r JoinGroupResp) []byte {
	var b []byte
	b = PutString(b, r.MemberID)
	b = PutInt32(b, r.GenerationID)
	b = PutString(b, r.LeaderID)
	b = PutInt32(b, int32(len(r.Members)))
	for _, m := range r.Members {
		b = PutString(b, m)
	}
	return b
}

func DecodeJoinGroupResp(buf []byte) (JoinGroupResp, error) {
	var r JoinGroupResp
	var err error
	r.MemberID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.GenerationID, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	r.LeaderID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	var n int32
	n, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	for i := int32(0); i < n; i++ {
		var m string
		m, buf, err = GetString(buf)
		if err != nil {
			return r, err
		}
		r.Members = append(r.Members, m)
	}
	return r, nil
}

type SyncGroupReq struct {
	GroupID      string
	MemberID     string
	GenerationID int32
	Assignments  map[string][]Assignment // memberID -> assignments (leader only)
}

func EncodeSyncGroupReq(r SyncGroupReq) []byte {
	var b []byte
	b = PutString(b, r.GroupID)
	b = PutString(b, r.MemberID)
	b = PutInt32(b, r.GenerationID)
	b = PutInt32(b, int32(len(r.Assignments)))
	for mid, as := range r.Assignments {
		b = PutString(b, mid)
		b = PutInt32(b, int32(len(as)))
		for _, a := range as {
			b = PutString(b, a.Topic)
			b = PutInt32(b, a.Partition)
		}
	}
	return b
}

func DecodeSyncGroupReq(buf []byte) (SyncGroupReq, error) {
	var r SyncGroupReq
	var err error
	r.GroupID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.MemberID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.GenerationID, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	var n int32
	n, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	r.Assignments = make(map[string][]Assignment)
	for i := int32(0); i < n; i++ {
		var mid string
		mid, buf, err = GetString(buf)
		if err != nil {
			return r, err
		}
		var na int32
		na, buf, err = GetInt32(buf)
		if err != nil {
			return r, err
		}
		var list []Assignment
		for j := int32(0); j < na; j++ {
			var a Assignment
			a.Topic, buf, err = GetString(buf)
			if err != nil {
				return r, err
			}
			a.Partition, buf, err = GetInt32(buf)
			if err != nil {
				return r, err
			}
			list = append(list, a)
		}
		r.Assignments[mid] = list
	}
	return r, nil
}

type SyncGroupResp struct {
	Assignments []Assignment
}

func EncodeSyncGroupResp(r SyncGroupResp) []byte {
	var b []byte
	b = PutInt32(b, int32(len(r.Assignments)))
	for _, a := range r.Assignments {
		b = PutString(b, a.Topic)
		b = PutInt32(b, a.Partition)
	}
	return b
}

func DecodeSyncGroupResp(buf []byte) (SyncGroupResp, error) {
	var r SyncGroupResp
	var err error
	var n int32
	n, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	for i := int32(0); i < n; i++ {
		var a Assignment
		a.Topic, buf, err = GetString(buf)
		if err != nil {
			return r, err
		}
		a.Partition, buf, err = GetInt32(buf)
		if err != nil {
			return r, err
		}
		r.Assignments = append(r.Assignments, a)
	}
	return r, nil
}

type HeartbeatReq struct {
	GroupID      string
	MemberID     string
	GenerationID int32
}

func EncodeHeartbeatReq(r HeartbeatReq) []byte {
	var b []byte
	b = PutString(b, r.GroupID)
	b = PutString(b, r.MemberID)
	b = PutInt32(b, r.GenerationID)
	return b
}

func DecodeHeartbeatReq(buf []byte) (HeartbeatReq, error) {
	var r HeartbeatReq
	var err error
	r.GroupID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.MemberID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.GenerationID, _, err = GetInt32(buf)
	return r, err
}

type LeaveGroupReq struct {
	GroupID  string
	MemberID string
}

func EncodeLeaveGroupReq(r LeaveGroupReq) []byte {
	var b []byte
	b = PutString(b, r.GroupID)
	b = PutString(b, r.MemberID)
	return b
}

func DecodeLeaveGroupReq(buf []byte) (LeaveGroupReq, error) {
	var r LeaveGroupReq
	var err error
	r.GroupID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.MemberID, _, err = GetString(buf)
	return r, err
}

type CommitOffsetReq struct {
	GroupID   string
	Topic     string
	Partition int32
	Offset    int64
}

func EncodeCommitOffsetReq(r CommitOffsetReq) []byte {
	var b []byte
	b = PutString(b, r.GroupID)
	b = PutString(b, r.Topic)
	b = PutInt32(b, r.Partition)
	b = PutInt64(b, r.Offset)
	return b
}

func DecodeCommitOffsetReq(buf []byte) (CommitOffsetReq, error) {
	var r CommitOffsetReq
	var err error
	r.GroupID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.Topic, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.Partition, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	r.Offset, _, err = GetInt64(buf)
	return r, err
}

type FetchOffsetsReq struct {
	GroupID string
	Topic   string
}

func EncodeFetchOffsetsReq(r FetchOffsetsReq) []byte {
	var b []byte
	b = PutString(b, r.GroupID)
	b = PutString(b, r.Topic)
	return b
}

func DecodeFetchOffsetsReq(buf []byte) (FetchOffsetsReq, error) {
	var r FetchOffsetsReq
	var err error
	r.GroupID, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.Topic, _, err = GetString(buf)
	return r, err
}

type OffsetEntry struct {
	Partition int32
	Offset    int64
}

type FetchOffsetsResp struct {
	Offsets []OffsetEntry
}

func EncodeFetchOffsetsResp(r FetchOffsetsResp) []byte {
	var b []byte
	b = PutInt32(b, int32(len(r.Offsets)))
	for _, e := range r.Offsets {
		b = PutInt32(b, e.Partition)
		b = PutInt64(b, e.Offset)
	}
	return b
}

func DecodeFetchOffsetsResp(buf []byte) (FetchOffsetsResp, error) {
	var r FetchOffsetsResp
	var err error
	var n int32
	n, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	for i := int32(0); i < n; i++ {
		var e OffsetEntry
		e.Partition, buf, err = GetInt32(buf)
		if err != nil {
			return r, err
		}
		e.Offset, buf, err = GetInt64(buf)
		if err != nil {
			return r, err
		}
		r.Offsets = append(r.Offsets, e)
	}
	return r, nil
}

// Replica append (leader -> follower)

type ReplicaAppendReq struct {
	Topic     string
	Partition int32
	Records   []Record
}

func EncodeReplicaAppendReq(r ReplicaAppendReq) []byte {
	var b []byte
	b = PutString(b, r.Topic)
	b = PutInt32(b, r.Partition)
	b = PutInt32(b, int32(len(r.Records)))
	for _, rec := range r.Records {
		b = PutInt64(b, rec.Offset)
		b = PutInt64(b, rec.Timestamp)
		b = append(b, rec.Attrs)
		b = PutBytes(b, rec.Key)
		b = PutBytes(b, rec.Value)
	}
	return b
}

func DecodeReplicaAppendReq(buf []byte) (ReplicaAppendReq, error) {
	var r ReplicaAppendReq
	var err error
	r.Topic, buf, err = GetString(buf)
	if err != nil {
		return r, err
	}
	r.Partition, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	var n int32
	n, buf, err = GetInt32(buf)
	if err != nil {
		return r, err
	}
	for i := int32(0); i < n; i++ {
		var rec Record
		rec.Offset, buf, err = GetInt64(buf)
		if err != nil {
			return r, err
		}
		rec.Timestamp, buf, err = GetInt64(buf)
		if err != nil {
			return r, err
		}
		if len(buf) < 1 {
			return r, errors.New("short attrs")
		}
		rec.Attrs = buf[0]
		buf = buf[1:]
		rec.Key, buf, err = GetBytes(buf)
		if err != nil {
			return r, err
		}
		rec.Value, buf, err = GetBytes(buf)
		if err != nil {
			return r, err
		}
		r.Records = append(r.Records, rec)
	}
	return r, nil
}

type ReplicaAppendResp struct {
	LastOffset int64
}

func EncodeReplicaAppendResp(r ReplicaAppendResp) []byte {
	return PutInt64(nil, r.LastOffset)
}

func DecodeReplicaAppendResp(buf []byte) (ReplicaAppendResp, error) {
	off, _, err := GetInt64(buf)
	return ReplicaAppendResp{LastOffset: off}, err
}
