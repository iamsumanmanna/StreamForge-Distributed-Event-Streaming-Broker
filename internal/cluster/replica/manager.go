package replica

import (
	"sync"
	"time"

	"github.com/distributed-system/streamforge/internal/transport/protocol"
	"github.com/distributed-system/streamforge/pkg/client"
)

// PeerAddrs maps nodeID -> broker TCP address (client port, not raft).
type PeerAddrs map[string]string

// Manager pushes leader appends to followers and tracks replica offsets for HWM.
type Manager struct {
	mu         sync.Mutex
	nodeID     string
	peers      PeerAddrs
	dialOpts   client.DialOptions
	replicaEnd map[string]int64 // key topic/partition/node -> last offset+1
	clients    map[string]*client.Client
}

func NewManager(nodeID string, peers PeerAddrs, dialOpts client.DialOptions) *Manager {
	return &Manager{
		nodeID:     nodeID,
		peers:      peers,
		dialOpts:   dialOpts,
		replicaEnd: make(map[string]int64),
		clients:    make(map[string]*client.Client),
	}
}

func key(topic string, part int32, node string) string {
	return topic + "/" + itoa(part) + "/" + node
}

func itoa(n int32) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func (m *Manager) getClient(nodeID string) (*client.Client, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.clients[nodeID]; ok {
		return c, nil
	}
	addr, ok := m.peers[nodeID]
	if !ok {
		return nil, errNoPeer
	}
	c, err := client.DialWithOptions(addr, m.dialOpts)
	if err != nil {
		return nil, err
	}
	m.clients[nodeID] = c
	return c, nil
}

var errNoPeer = errString("unknown peer")

type errString string

func (e errString) Error() string { return string(e) }

// Replicate sends records to followers and returns majority-acked end offset (exclusive).
func (m *Manager) Replicate(topic string, partition int32, leaderEnd int64, replicas []string, recs []protocol.Record) int64 {
	if len(recs) == 0 {
		return leaderEnd
	}
	acks := 1 // leader
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, r := range replicas {
		if r == m.nodeID {
			continue
		}
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			c, err := m.getClient(node)
			if err != nil {
				return
			}
			last, err := c.ReplicaAppend(protocol.ReplicaAppendReq{
				Topic: topic, Partition: partition, Records: recs,
			})
			if err != nil {
				m.mu.Lock()
				if old, ok := m.clients[node]; ok {
					_ = old.Close()
					delete(m.clients, node)
				}
				m.mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				c, err = m.getClient(node)
				if err != nil {
					return
				}
				last, err = c.ReplicaAppend(protocol.ReplicaAppendReq{
					Topic: topic, Partition: partition, Records: recs,
				})
				if err != nil {
					return
				}
			}
			m.mu.Lock()
			m.replicaEnd[key(topic, partition, node)] = last + 1
			mu.Lock()
			acks++
			mu.Unlock()
			m.mu.Unlock()
		}(r)
	}
	wg.Wait()

	majority := (len(replicas) / 2) + 1
	return m.computeHWM(topic, partition, leaderEnd, replicas, majority)
}

func (m *Manager) computeHWM(topic string, partition int32, leaderEnd int64, replicas []string, majority int) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	ends := []int64{leaderEnd}
	for _, r := range replicas {
		if r == m.nodeID {
			continue
		}
		if e, ok := m.replicaEnd[key(topic, partition, r)]; ok {
			ends = append(ends, e)
		}
	}
	if len(ends) < majority {
		if len(ends) == 1 {
			return ends[0]
		}
	}
	for i := 0; i < len(ends); i++ {
		for j := i + 1; j < len(ends); j++ {
			if ends[j] > ends[i] {
				ends[i], ends[j] = ends[j], ends[i]
			}
		}
	}
	idx := majority - 1
	if idx >= len(ends) {
		idx = len(ends) - 1
	}
	if idx < 0 {
		return 0
	}
	return ends[idx]
}

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.clients {
		_ = c.Close()
	}
}
