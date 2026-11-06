package raftmeta

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// PartitionAssignment describes leadership for one partition.
type PartitionAssignment struct {
	Topic     string   `json:"topic"`
	Partition int32    `json:"partition"`
	Leader    string   `json:"leader"`
	Replicas  []string `json:"replicas"`
}

// Snapshot is the replicated metadata FSM state.
type Snapshot struct {
	Topics map[string]TopicInfo `json:"topics"`
}

type TopicInfo struct {
	Partitions int32                 `json:"partitions"`
	Assign     []PartitionAssignment `json:"assign"`
}

type command struct {
	Op     string                `json:"op"`
	Topic  string                `json:"topic,omitempty"`
	Parts  int32                 `json:"parts,omitempty"`
	RF     int                   `json:"rf,omitempty"`
	Node   string                `json:"node,omitempty"`
	Nodes  []string              `json:"nodes,omitempty"`
	Assign []PartitionAssignment `json:"assign,omitempty"`
}

type fsm struct {
	mu   sync.RWMutex
	data Snapshot
}

func newFSM() *fsm {
	return &fsm{data: Snapshot{Topics: make(map[string]TopicInfo)}}
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch cmd.Op {
	case "create_topic":
		if _, ok := f.data.Topics[cmd.Topic]; ok {
			return nil
		}
		info := TopicInfo{Partitions: cmd.Parts, Assign: cmd.Assign}
		f.data.Topics[cmd.Topic] = info
	case "set_assign":
		info := f.data.Topics[cmd.Topic]
		info.Assign = cmd.Assign
		f.data.Topics[cmd.Topic] = info
	}
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, err := json.Marshal(f.data)
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: b}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var snap Snapshot
	dec := json.NewDecoder(rc)
	if err := dec.Decode(&snap); err != nil {
		return err
	}
	if snap.Topics == nil {
		snap.Topics = make(map[string]TopicInfo)
	}
	f.mu.Lock()
	f.data = snap
	f.mu.Unlock()
	return nil
}

type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// Store is a Raft-backed metadata store for topic/partition leadership.
type Store struct {
	nodeID  string
	raft    *raft.Raft
	fsm     *fsm
	nodes   []string // known broker node IDs
	peerMap map[string]string
}

// Peer is nodeID=host:raftPort
func ParsePeers(peers string) map[string]string {
	out := map[string]string{}
	if peers == "" {
		return out
	}
	for _, p := range strings.Split(peers, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		parts := strings.SplitN(p, "=", 2)
		if len(parts) != 2 {
			continue
		}
		out[parts[0]] = parts[1]
	}
	return out
}

func Open(nodeID, raftBind, dataDir, peersCSV string, bootstrap bool) (*Store, error) {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(nodeID)
	cfg.SnapshotInterval = 30 * time.Second
	cfg.SnapshotThreshold = 1000

	addr, err := net.ResolveTCPAddr("tcp", raftBind)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(raftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	raftDir := filepath.Join(dataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, err
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-log.db"))
	if err != nil {
		return nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-stable.db"))
	if err != nil {
		return nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	f := newFSM()
	r, err := raft.NewRaft(cfg, f, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	peerMap := ParsePeers(peersCSV)
	nodes := make([]string, 0, len(peerMap)+1)
	nodes = append(nodes, nodeID)
	for id := range peerMap {
		if id != nodeID {
			nodes = append(nodes, id)
		}
	}

	s := &Store{nodeID: nodeID, raft: r, fsm: f, nodes: nodes, peerMap: peerMap}

	if bootstrap {
		f := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{
			ID:      raft.ServerID(nodeID),
			Address: transport.LocalAddr(),
		}}})
		if err := f.Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, err
		}
		go s.addPeersLoop()
	}
	return s, nil
}

func (s *Store) addPeersLoop() {
	for i := 0; i < 60; i++ {
		time.Sleep(500 * time.Millisecond)
		if s.raft.State() != raft.Leader {
			continue
		}
		for id, addr := range s.peerMap {
			if id == s.nodeID {
				continue
			}
			f := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 5*time.Second)
			_ = f.Error()
		}
		return
	}
}

func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *Store) Leader() string {
	addr, id := s.raft.LeaderWithID()
	_ = addr
	return string(id)
}

func (s *Store) apply(cmd command) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := s.raft.Apply(b, 5*time.Second)
	return f.Error()
}

func (s *Store) CreateTopic(topic string, partitions int32, rf int) error {
	if rf <= 0 {
		rf = 1
	}
	if rf > len(s.nodes) {
		rf = len(s.nodes)
	}
	assigns := make([]PartitionAssignment, 0, partitions)
	for i := int32(0); i < partitions; i++ {
		replicas := make([]string, 0, rf)
		for r := 0; r < rf; r++ {
			replicas = append(replicas, s.nodes[int(i+int32(r))%len(s.nodes)])
		}
		assigns = append(assigns, PartitionAssignment{
			Topic:     topic,
			Partition: i,
			Leader:    replicas[0],
			Replicas:  replicas,
		})
	}
	return s.apply(command{Op: "create_topic", Topic: topic, Parts: partitions, RF: rf, Assign: assigns})
}

func (s *Store) Snapshot() Snapshot {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	// shallow copy
	out := Snapshot{Topics: make(map[string]TopicInfo, len(s.fsm.data.Topics))}
	for k, v := range s.fsm.data.Topics {
		out.Topics[k] = v
	}
	return out
}

func (s *Store) LeaderFor(topic string, partition int32) (string, bool) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	info, ok := s.fsm.data.Topics[topic]
	if !ok {
		return "", false
	}
	for _, a := range info.Assign {
		if a.Partition == partition {
			return a.Leader, true
		}
	}
	return "", false
}

func (s *Store) ReplicasFor(topic string, partition int32) ([]string, bool) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	info, ok := s.fsm.data.Topics[topic]
	if !ok {
		return nil, false
	}
	for _, a := range info.Assign {
		if a.Partition == partition {
			return append([]string(nil), a.Replicas...), true
		}
	}
	return nil, false
}

func (s *Store) TransferLeadershipOnShutdown() {
	_ = s.raft.Shutdown().Error()
}

func (s *Store) Nodes() []string {
	return append([]string(nil), s.nodes...)
}
