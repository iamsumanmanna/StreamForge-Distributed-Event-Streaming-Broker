package client

import (
	"crypto/tls"
	"fmt"
	"hash/fnv"
	"net"
	"sync/atomic"
	"time"

	"github.com/distributed-system/streamforge/internal/transport/protocol"
)

// DialOptions configures TLS and auth for broker connections.
type DialOptions struct {
	TLS        bool
	Insecure   bool // skip cert verify (dev / self-signed)
	ServerName string
	AuthToken  string
	Timeout    time.Duration
}

// Client is a TCP client for the StreamForge Distributed Event Streaming Broker protocol.
type Client struct {
	addr string
	conn net.Conn
	corr uint32
	opts DialOptions
}

func Dial(addr string) (*Client, error) {
	return DialWithOptions(addr, DialOptions{})
}

func DialWithOptions(addr string, opts DialOptions) (*Client, error) {
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	var conn net.Conn
	var err error
	if opts.TLS {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: opts.Insecure, //nolint:gosec // intentional for local/dev
			ServerName:         opts.ServerName,
		}
		d := &net.Dialer{Timeout: opts.Timeout}
		conn, err = tls.DialWithDialer(d, "tcp", addr, tlsCfg)
	} else {
		conn, err = net.DialTimeout("tcp", addr, opts.Timeout)
	}
	if err != nil {
		return nil, err
	}
	c := &Client{addr: addr, conn: conn, opts: opts}
	if opts.AuthToken != "" {
		if err := c.Auth(opts.AuthToken); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) nextCorr() uint32 {
	return atomic.AddUint32(&c.corr, 1)
}

func (c *Client) roundTrip(apiKey uint16, payload []byte) (uint16, []byte, error) {
	corr := c.nextCorr()
	body := protocol.EncodeRequest(apiKey, 1, corr, payload)
	_ = c.conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err := protocol.EncodeFrame(c.conn, body); err != nil {
		return 0, nil, err
	}
	frame, err := protocol.DecodeFrame(c.conn)
	if err != nil {
		return 0, nil, err
	}
	gotCorr, errCode, respPayload, err := protocol.DecodeResponse(frame)
	if err != nil {
		return 0, nil, err
	}
	if gotCorr != corr {
		return 0, nil, fmt.Errorf("correlation mismatch")
	}
	return errCode, respPayload, nil
}

func (c *Client) Auth(token string) error {
	code, _, err := c.roundTrip(protocol.APIAuth, protocol.EncodeAuthReq(protocol.AuthReq{Token: token}))
	if err != nil {
		return err
	}
	if code == protocol.ErrUnauthorized {
		return fmt.Errorf("unauthorized")
	}
	if code != protocol.ErrNone {
		return fmt.Errorf("auth error code %d", code)
	}
	return nil
}

func (c *Client) CreateTopic(topic string, partitions int32) error {
	code, _, err := c.roundTrip(protocol.APICreateTopic, protocol.EncodeCreateTopicReq(protocol.CreateTopicReq{
		Topic: topic, Partitions: partitions,
	}))
	if err != nil {
		return err
	}
	if code != protocol.ErrNone && code != protocol.ErrTopicExists {
		return fmt.Errorf("create topic error code %d", code)
	}
	return nil
}

func (c *Client) Produce(topic string, partition int32, key, value []byte) (int64, error) {
	return c.ProduceOpts(topic, partition, key, value, protocol.AcksLeader, 0)
}

func (c *Client) ProduceOpts(topic string, partition int32, key, value []byte, acks int16, compression uint8) (int64, error) {
	code, payload, err := c.roundTrip(protocol.APIProduce, protocol.EncodeProduceReq(protocol.ProduceReq{
		Topic: topic, Partition: partition, Key: key, Value: value,
		Acks: acks, Compression: compression,
	}))
	if err != nil {
		return 0, err
	}
	if code != protocol.ErrNone {
		return 0, fmt.Errorf("produce error code %d", code)
	}
	resp, err := protocol.DecodeProduceResp(payload)
	return resp.Offset, err
}

// ProduceAuto picks partition by key hash (or 0 if no key / unknown topic uses partition 0).
func (c *Client) ProduceAuto(topic string, key, value []byte, numPartitions int32) (int64, int32, error) {
	var part int32
	if numPartitions <= 0 {
		numPartitions = 1
	}
	if len(key) > 0 {
		h := fnv.New32a()
		_, _ = h.Write(key)
		part = int32(h.Sum32() % uint32(numPartitions))
	}
	off, err := c.Produce(topic, part, key, value)
	return off, part, err
}

func (c *Client) Fetch(topic string, partition int32, offset int64, maxBytes int32) (protocol.FetchResp, error) {
	code, payload, err := c.roundTrip(protocol.APIFetch, protocol.EncodeFetchReq(protocol.FetchReq{
		Topic: topic, Partition: partition, Offset: offset, MaxBytes: maxBytes,
	}))
	if err != nil {
		return protocol.FetchResp{}, err
	}
	if code != protocol.ErrNone {
		return protocol.FetchResp{}, fmt.Errorf("fetch error code %d", code)
	}
	return protocol.DecodeFetchResp(payload)
}

func (c *Client) Metadata() (protocol.MetadataResp, error) {
	code, payload, err := c.roundTrip(protocol.APIMetadata, nil)
	if err != nil {
		return protocol.MetadataResp{}, err
	}
	if code != protocol.ErrNone {
		return protocol.MetadataResp{}, fmt.Errorf("metadata error code %d", code)
	}
	return protocol.DecodeMetadataResp(payload)
}

func (c *Client) JoinGroup(groupID, memberID string, topics []string) (protocol.JoinGroupResp, error) {
	code, payload, err := c.roundTrip(protocol.APIJoinGroup, protocol.EncodeJoinGroupReq(protocol.JoinGroupReq{
		GroupID: groupID, MemberID: memberID, Topics: topics,
	}))
	if err != nil {
		return protocol.JoinGroupResp{}, err
	}
	if code != protocol.ErrNone {
		return protocol.JoinGroupResp{}, fmt.Errorf("join group error %d", code)
	}
	return protocol.DecodeJoinGroupResp(payload)
}

func (c *Client) SyncGroup(req protocol.SyncGroupReq) (protocol.SyncGroupResp, error) {
	code, payload, err := c.roundTrip(protocol.APISyncGroup, protocol.EncodeSyncGroupReq(req))
	if err != nil {
		return protocol.SyncGroupResp{}, err
	}
	if code != protocol.ErrNone {
		return protocol.SyncGroupResp{}, fmt.Errorf("sync group error %d", code)
	}
	return protocol.DecodeSyncGroupResp(payload)
}

func (c *Client) Heartbeat(groupID, memberID string, gen int32) error {
	code, _, err := c.roundTrip(protocol.APIHeartbeat, protocol.EncodeHeartbeatReq(protocol.HeartbeatReq{
		GroupID: groupID, MemberID: memberID, GenerationID: gen,
	}))
	if err != nil {
		return err
	}
	if code != protocol.ErrNone {
		return fmt.Errorf("heartbeat error %d", code)
	}
	return nil
}

func (c *Client) LeaveGroup(groupID, memberID string) error {
	code, _, err := c.roundTrip(protocol.APILeaveGroup, protocol.EncodeLeaveGroupReq(protocol.LeaveGroupReq{
		GroupID: groupID, MemberID: memberID,
	}))
	if err != nil {
		return err
	}
	if code != protocol.ErrNone {
		return fmt.Errorf("leave group error %d", code)
	}
	return nil
}

func (c *Client) CommitOffset(groupID, topic string, partition int32, offset int64) error {
	code, _, err := c.roundTrip(protocol.APICommitOffset, protocol.EncodeCommitOffsetReq(protocol.CommitOffsetReq{
		GroupID: groupID, Topic: topic, Partition: partition, Offset: offset,
	}))
	if err != nil {
		return err
	}
	if code != protocol.ErrNone {
		return fmt.Errorf("commit offset error %d", code)
	}
	return nil
}

func (c *Client) FetchOffsets(groupID, topic string) (protocol.FetchOffsetsResp, error) {
	code, payload, err := c.roundTrip(protocol.APIFetchOffsets, protocol.EncodeFetchOffsetsReq(protocol.FetchOffsetsReq{
		GroupID: groupID, Topic: topic,
	}))
	if err != nil {
		return protocol.FetchOffsetsResp{}, err
	}
	if code != protocol.ErrNone {
		return protocol.FetchOffsetsResp{}, fmt.Errorf("fetch offsets error %d", code)
	}
	return protocol.DecodeFetchOffsetsResp(payload)
}

func (c *Client) ReplicaAppend(req protocol.ReplicaAppendReq) (int64, error) {
	code, payload, err := c.roundTrip(protocol.APIReplicaAppend, protocol.EncodeReplicaAppendReq(req))
	if err != nil {
		return 0, err
	}
	if code != protocol.ErrNone {
		return 0, fmt.Errorf("replica append error %d", code)
	}
	resp, err := protocol.DecodeReplicaAppendResp(payload)
	return resp.LastOffset, err
}
