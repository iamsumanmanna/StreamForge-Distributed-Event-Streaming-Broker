package broker

import (
	"errors"
	"hash/fnv"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/distributed-system/streamforge/internal/cluster/raftmeta"
	"github.com/distributed-system/streamforge/internal/cluster/replica"
	"github.com/distributed-system/streamforge/internal/config"
	"github.com/distributed-system/streamforge/internal/consumer/group"
	"github.com/distributed-system/streamforge/internal/storage/compress"
	"github.com/distributed-system/streamforge/internal/storage/log"
	"github.com/distributed-system/streamforge/internal/transport/protocol"
	"github.com/distributed-system/streamforge/pkg/client"
	"github.com/prometheus/client_golang/prometheus"
)

func clientDialOpts(cfg config.Config) client.DialOptions {
	return client.DialOptions{
		TLS:       cfg.TLSCertFile != "" && cfg.TLSKeyFile != "",
		Insecure:  true, // cluster-internal; use proper CA in production
		AuthToken: cfg.AuthToken,
	}
}

// Broker is the core request handler.
type Broker struct {
	cfg      config.Config
	topics   *log.TopicManager
	groups   *group.Coordinator
	meta     *raftmeta.Store // optional
	replicas *replica.Manager
	log      *slog.Logger

	produceTotal prometheus.Counter
	fetchTotal   prometheus.Counter
	produceBytes prometheus.Counter

	// local leadership when not in cluster mode: this node leads everything
	mu sync.RWMutex
}

func New(cfg config.Config, logger *slog.Logger) (*Broker, error) {
	opts := log.Options{
		SegmentMaxBytes: cfg.SegmentMaxBytes,
		FsyncEveryN:     cfg.FsyncEveryN,
		FsyncInterval:   cfg.FsyncInterval,
		RetentionBytes:  cfg.RetentionBytes,
		RetentionAge:    cfg.RetentionAge,
	}
	tm, err := log.NewTopicManager(cfg.DataDir+"/topics", opts)
	if err != nil {
		return nil, err
	}
	b := &Broker{
		cfg:    cfg,
		topics: tm,
		log:    logger,
		produceTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamforge_produce_total",
			Help: "Total produce requests",
		}),
		fetchTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamforge_fetch_total",
			Help: "Total fetch requests",
		}),
		produceBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamforge_produce_bytes_total",
			Help: "Total produced bytes",
		}),
	}
	prometheus.MustRegister(b.produceTotal, b.fetchTotal, b.produceBytes)
	groups, err := group.NewCoordinator(cfg.DataDir, func(topic string) (int32, error) {
		return tm.PartitionCount(topic)
	})
	if err != nil {
		return nil, err
	}
	b.groups = groups

	peers := raftmeta.ParsePeers(cfg.ClusterPeers)
	clientPeers := replica.PeerAddrs{}
	pcs := os.Getenv("SF_PEER_CLIENTS")
	if pcs == "" {
		pcs = os.Getenv("MK_PEER_CLIENTS")
	}
	if pcs != "" {
		for id, addr := range raftmeta.ParsePeers(pcs) {
			clientPeers[id] = addr
		}
	} else {
		for id, raftAddr := range peers {
			clientPeers[id] = rewriteRaftToClient(raftAddr)
		}
	}
	dialOpts := clientDialOpts(cfg)
	b.replicas = replica.NewManager(cfg.NodeID, clientPeers, dialOpts)

	if cfg.EnableCluster {
		// Only the lexicographically first peer bootstraps the Raft cluster.
		bootstrap := shouldBootstrap(cfg.NodeID, peers)
		store, err := raftmeta.Open(cfg.NodeID, cfg.RaftBind, cfg.DataDir, cfg.ClusterPeers, bootstrap)
		if err != nil {
			return nil, err
		}
		b.meta = store
	}
	return b, nil
}

func shouldBootstrap(nodeID string, peers map[string]string) bool {
	if len(peers) == 0 {
		return true
	}
	ids := make([]string, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	if _, ok := peers[nodeID]; !ok {
		ids = append(ids, nodeID)
	}
	first := ids[0]
	for _, id := range ids {
		if id < first {
			first = id
		}
	}
	boot := os.Getenv("SF_BOOTSTRAP")
	if boot == "" {
		boot = os.Getenv("MK_BOOTSTRAP")
	}
	return nodeID == first || strings.EqualFold(boot, "true")
}

func rewriteRaftToClient(raftAddr string) string {
	// ":7000" / "host:7000" -> host:9092 by replacing port 7000 with 9092, 7001->9093, 7002->9094
	host, port, ok := splitHostPort(raftAddr)
	if !ok {
		return raftAddr
	}
	switch port {
	case "7000":
		return host + ":9092"
	case "7001":
		return host + ":9093"
	case "7002":
		return host + ":9094"
	default:
		return host + ":9092"
	}
}

func splitHostPort(addr string) (host, port string, ok bool) {
	if addr == "" {
		return "", "", false
	}
	if addr[0] == ':' {
		return "127.0.0.1", addr[1:], true
	}
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i], addr[i+1:], true
		}
	}
	return "", "", false
}

func (b *Broker) Close() error {
	if b.replicas != nil {
		b.replicas.Close()
	}
	if b.meta != nil {
		b.meta.TransferLeadershipOnShutdown()
	}
	return b.topics.Close()
}

func (b *Broker) AuthToken() string { return b.cfg.AuthToken }

func (b *Broker) Handle(apiKey uint16, payload []byte) (uint16, []byte) {
	switch apiKey {
	case protocol.APICreateTopic:
		return b.handleCreateTopic(payload)
	case protocol.APIProduce:
		return b.handleProduce(payload)
	case protocol.APIFetch:
		return b.handleFetch(payload)
	case protocol.APIMetadata:
		return b.handleMetadata()
	case protocol.APIJoinGroup:
		return b.handleJoin(payload)
	case protocol.APISyncGroup:
		return b.handleSync(payload)
	case protocol.APIHeartbeat:
		return b.handleHeartbeat(payload)
	case protocol.APILeaveGroup:
		return b.handleLeave(payload)
	case protocol.APICommitOffset:
		return b.handleCommit(payload)
	case protocol.APIFetchOffsets:
		return b.handleFetchOffsets(payload)
	case protocol.APIReplicaAppend:
		return b.handleReplicaAppend(payload)
	case protocol.APIAuth:
		return b.handleAuth(payload)
	default:
		return protocol.ErrUnknown, nil
	}
}

func (b *Broker) handleAuth(payload []byte) (uint16, []byte) {
	req, err := protocol.DecodeAuthReq(payload)
	if err != nil {
		return protocol.ErrInvalidRequest, nil
	}
	if b.cfg.AuthToken == "" || req.Token == b.cfg.AuthToken {
		return protocol.ErrNone, nil
	}
	return protocol.ErrUnauthorized, nil
}

func (b *Broker) handleCreateTopic(payload []byte) (uint16, []byte) {
	req, err := protocol.DecodeCreateTopicReq(payload)
	if err != nil {
		return protocol.ErrInvalidRequest, nil
	}
	if b.meta != nil {
		if !b.meta.IsLeader() {
			return protocol.ErrNotLeader, nil
		}
		if err := b.meta.CreateTopic(req.Topic, req.Partitions, b.cfg.ReplicationFactor); err != nil {
			b.log.Error("raft create topic", "err", err)
			return protocol.ErrUnknown, nil
		}
	}
	if err := b.topics.CreateTopic(req.Topic, req.Partitions); err != nil {
		if errors.Is(err, log.ErrTopicExists) {
			return protocol.ErrTopicExists, nil
		}
		return protocol.ErrUnknown, nil
	}
	// Ensure local partitions exist on all nodes via metadata apply side-effect:
	// followers learn topics via Metadata/CreateTopic local create when they see metadata.
	return protocol.ErrNone, nil
}

func (b *Broker) ensureLocalTopic(topic string, partitions int32) {
	if _, err := b.topics.PartitionCount(topic); err == nil {
		return
	}
	_ = b.topics.CreateTopic(topic, partitions)
}

func (b *Broker) isLeader(topic string, partition int32) bool {
	if b.meta == nil {
		return true
	}
	leader, ok := b.meta.LeaderFor(topic, partition)
	if !ok {
		return true
	}
	return leader == b.cfg.NodeID
}

func (b *Broker) handleProduce(payload []byte) (uint16, []byte) {
	req, err := protocol.DecodeProduceReq(payload)
	if err != nil {
		return protocol.ErrInvalidRequest, nil
	}
	b.produceTotal.Inc()
	b.produceBytes.Add(float64(len(req.Key) + len(req.Value)))

	acks := req.Acks
	if acks != protocol.AcksNone && acks != protocol.AcksLeader && acks != protocol.AcksAll {
		acks = b.cfg.DefaultAcks
	}
	codec := req.Compression
	if codec == 0 && b.cfg.DefaultCompression != "" && b.cfg.DefaultCompression != "none" {
		if c, err := compress.Parse(b.cfg.DefaultCompression); err == nil {
			codec = c
		}
	}

	if req.Partition < 0 {
		n, err := b.topics.PartitionCount(req.Topic)
		if err != nil {
			return protocol.ErrUnknownTopic, nil
		}
		if len(req.Key) > 0 {
			h := fnv.New32a()
			_, _ = h.Write(req.Key)
			req.Partition = int32(h.Sum32() % uint32(n))
		} else {
			req.Partition = 0
		}
	}

	if !b.isLeader(req.Topic, req.Partition) {
		return protocol.ErrNotLeader, nil
	}

	part, err := b.topics.Get(req.Topic, req.Partition)
	if err != nil {
		if errors.Is(err, log.ErrUnknownTopic) {
			return protocol.ErrUnknownTopic, nil
		}
		return protocol.ErrUnknownPartition, nil
	}

	storedVal := req.Value
	if codec != compress.None {
		storedVal, err = compress.Encode(codec, req.Value)
		if err != nil {
			return protocol.ErrUnknown, nil
		}
	}

	ts := time.Now().UnixMilli()
	off, err := part.AppendWithTime(ts, codec, req.Key, storedVal)
	if err != nil {
		return protocol.ErrUnknown, nil
	}

	resp := protocol.EncodeProduceResp(protocol.ProduceResp{Offset: off})

	if acks == protocol.AcksNone {
		// Fire-and-forget: still advance HWM locally so consumers can read.
		part.SetHighWatermark(part.NextOffset())
		return protocol.ErrNone, resp
	}

	// Replication for cluster + acks=all
	if b.meta != nil {
		replicas, ok := b.meta.ReplicasFor(req.Topic, req.Partition)
		if ok && len(replicas) > 1 {
			recs := []protocol.Record{{
				Offset: off, Timestamp: ts, Attrs: codec, Key: req.Key, Value: storedVal,
			}}
			if acks == protocol.AcksAll {
				hwm := b.replicas.Replicate(req.Topic, req.Partition, part.NextOffset(), replicas, recs)
				part.SetHighWatermark(hwm)
			} else {
				// acks=1: leader only; kick off async replicate without waiting
				go b.replicas.Replicate(req.Topic, req.Partition, part.NextOffset(), replicas, recs)
				part.SetHighWatermark(part.NextOffset())
			}
			return protocol.ErrNone, resp
		}
	}

	part.SetHighWatermark(part.NextOffset())
	return protocol.ErrNone, resp
}

func (b *Broker) handleFetch(payload []byte) (uint16, []byte) {
	req, err := protocol.DecodeFetchReq(payload)
	if err != nil {
		return protocol.ErrInvalidRequest, nil
	}
	b.fetchTotal.Inc()
	part, err := b.topics.Get(req.Topic, req.Partition)
	if err != nil {
		if errors.Is(err, log.ErrUnknownTopic) {
			return protocol.ErrUnknownTopic, nil
		}
		return protocol.ErrUnknownPartition, nil
	}
	max := int(req.MaxBytes)
	if max <= 0 {
		max = 1 << 20
	}
	recs, hwm, err := part.Read(req.Offset, max)
	if err != nil {
		if errors.Is(err, log.ErrOffsetOutOfRange) {
			return protocol.ErrOffsetOutOfRange, nil
		}
		return protocol.ErrUnknown, nil
	}
	for i := range recs {
		if recs[i].Attrs&0x07 != compress.None {
			plain, derr := compress.Decode(recs[i].Attrs, recs[i].Value)
			if derr != nil {
				return protocol.ErrUnknown, nil
			}
			recs[i].Value = plain
			recs[i].Attrs = compress.None // clients receive plaintext
		}
	}
	return protocol.ErrNone, protocol.EncodeFetchResp(protocol.FetchResp{
		HighWatermark: hwm,
		Records:       recs,
	})
}

func (b *Broker) handleMetadata() (uint16, []byte) {
	resp := protocol.MetadataResp{NodeID: b.cfg.NodeID}
	if b.meta != nil {
		snap := b.meta.Snapshot()
		for name, info := range snap.Topics {
			b.ensureLocalTopic(name, info.Partitions)
			tm := protocol.TopicMeta{Name: name}
			for _, a := range info.Assign {
				tm.Partitions = append(tm.Partitions, protocol.PartitionMeta{
					ID: a.Partition, Leader: a.Leader,
				})
			}
			resp.Topics = append(resp.Topics, tm)
		}
		return protocol.ErrNone, protocol.EncodeMetadataResp(resp)
	}
	for name, ids := range b.topics.ListTopics() {
		tm := protocol.TopicMeta{Name: name}
		for _, id := range ids {
			tm.Partitions = append(tm.Partitions, protocol.PartitionMeta{
				ID: id, Leader: b.cfg.NodeID,
			})
		}
		resp.Topics = append(resp.Topics, tm)
	}
	return protocol.ErrNone, protocol.EncodeMetadataResp(resp)
}

func (b *Broker) handleJoin(payload []byte) (uint16, []byte) {
	req, err := protocol.DecodeJoinGroupReq(payload)
	if err != nil {
		return protocol.ErrInvalidRequest, nil
	}
	resp, code := b.groups.Join(req)
	return code, protocol.EncodeJoinGroupResp(resp)
}

func (b *Broker) handleSync(payload []byte) (uint16, []byte) {
	req, err := protocol.DecodeSyncGroupReq(payload)
	if err != nil {
		return protocol.ErrInvalidRequest, nil
	}
	resp, code := b.groups.Sync(req)
	return code, protocol.EncodeSyncGroupResp(resp)
}

func (b *Broker) handleHeartbeat(payload []byte) (uint16, []byte) {
	req, err := protocol.DecodeHeartbeatReq(payload)
	if err != nil {
		return protocol.ErrInvalidRequest, nil
	}
	return b.groups.Heartbeat(req), nil
}

func (b *Broker) handleLeave(payload []byte) (uint16, []byte) {
	req, err := protocol.DecodeLeaveGroupReq(payload)
	if err != nil {
		return protocol.ErrInvalidRequest, nil
	}
	return b.groups.Leave(req), nil
}

func (b *Broker) handleCommit(payload []byte) (uint16, []byte) {
	req, err := protocol.DecodeCommitOffsetReq(payload)
	if err != nil {
		return protocol.ErrInvalidRequest, nil
	}
	return b.groups.Commit(req), nil
}

func (b *Broker) handleFetchOffsets(payload []byte) (uint16, []byte) {
	req, err := protocol.DecodeFetchOffsetsReq(payload)
	if err != nil {
		return protocol.ErrInvalidRequest, nil
	}
	resp := b.groups.FetchOffsets(req.GroupID, req.Topic)
	return protocol.ErrNone, protocol.EncodeFetchOffsetsResp(resp)
}

func (b *Broker) handleReplicaAppend(payload []byte) (uint16, []byte) {
	req, err := protocol.DecodeReplicaAppendReq(payload)
	if err != nil {
		return protocol.ErrInvalidRequest, nil
	}
	part, err := b.topics.EnsurePartition(req.Topic, req.Partition)
	if err != nil {
		return protocol.ErrUnknown, nil
	}
	var last int64 = -1
	for _, rec := range req.Records {
		ts := rec.Timestamp
		if err := part.AppendAt(rec.Offset, ts, rec.Attrs, rec.Key, rec.Value); err != nil {
			b.log.Error("replica append", "err", err)
			return protocol.ErrUnknown, nil
		}
		last = rec.Offset
	}
	part.SetHighWatermark(part.NextOffset())
	return protocol.ErrNone, protocol.EncodeReplicaAppendResp(protocol.ReplicaAppendResp{LastOffset: last})
}
