package group

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/distributed-system/streamforge/internal/transport/protocol"
)

const sessionTimeout = 10 * time.Second

type member struct {
	id       string
	topics   []string
	lastBeat time.Time
	assigned []protocol.Assignment
}

type groupState struct {
	id           string
	members      map[string]*member
	generation   int32
	leaderID     string
	assignments  map[string][]protocol.Assignment
	state        string // empty, joining, stable
	joinDeadline time.Time
}

// Coordinator manages consumer groups and committed offsets.
type Coordinator struct {
	mu      sync.Mutex
	groups  map[string]*groupState
	offsets map[string]map[string]map[int32]int64 // group -> topic -> partition -> offset
	store   *offsetStore
	// partitionCounts looks up topic partition counts for assignment
	partitionCounts func(topic string) (int32, error)
}

func NewCoordinator(dataDir string, partitionCounts func(topic string) (int32, error)) (*Coordinator, error) {
	store, err := newOffsetStore(filepath.Join(dataDir, "groups"))
	if err != nil {
		return nil, err
	}
	offsets, err := store.load()
	if err != nil {
		return nil, err
	}
	c := &Coordinator{
		groups:          make(map[string]*groupState),
		offsets:         offsets,
		store:           store,
		partitionCounts: partitionCounts,
	}
	go c.reaper()
	return c, nil
}

func (c *Coordinator) reaper() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for range t.C {
		c.mu.Lock()
		now := time.Now()
		for _, g := range c.groups {
			for id, m := range g.members {
				if now.Sub(m.lastBeat) > sessionTimeout {
					delete(g.members, id)
					g.state = "joining"
					g.generation++
				}
			}
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) getOrCreate(id string) *groupState {
	g, ok := c.groups[id]
	if !ok {
		g = &groupState{
			id:          id,
			members:     make(map[string]*member),
			assignments: make(map[string][]protocol.Assignment),
			state:       "joining",
		}
		c.groups[id] = g
	}
	return g
}

func (c *Coordinator) Join(req protocol.JoinGroupReq) (protocol.JoinGroupResp, uint16) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.getOrCreate(req.GroupID)
	memberID := req.MemberID
	if memberID == "" {
		memberID = fmt.Sprintf("%s-%d", req.GroupID, time.Now().UnixNano())
	}
	m, ok := g.members[memberID]
	if !ok {
		m = &member{id: memberID}
		g.members[memberID] = m
		g.state = "joining"
		g.generation++
		g.joinDeadline = time.Now().Add(1 * time.Second)
	}
	m.topics = req.Topics
	m.lastBeat = time.Now()

	// Elect first joiner as leader
	if g.leaderID == "" || g.members[g.leaderID] == nil {
		g.leaderID = memberID
	}

	members := make([]string, 0, len(g.members))
	for id := range g.members {
		members = append(members, id)
	}
	sort.Strings(members)

	return protocol.JoinGroupResp{
		MemberID:     memberID,
		GenerationID: g.generation,
		LeaderID:     g.leaderID,
		Members:      members,
	}, protocol.ErrNone
}

func (c *Coordinator) Sync(req protocol.SyncGroupReq) (protocol.SyncGroupResp, uint16) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[req.GroupID]
	if !ok {
		return protocol.SyncGroupResp{}, protocol.ErrUnknownMember
	}
	m, ok := g.members[req.MemberID]
	if !ok {
		return protocol.SyncGroupResp{}, protocol.ErrUnknownMember
	}
	m.lastBeat = time.Now()
	if req.GenerationID != g.generation {
		return protocol.SyncGroupResp{}, protocol.ErrRebalanceInProg
	}

	if req.MemberID == g.leaderID && len(req.Assignments) > 0 {
		g.assignments = req.Assignments
		g.state = "stable"
	} else if req.MemberID == g.leaderID && len(req.Assignments) == 0 {
		// Auto range-assign if leader sent empty assignments
		g.assignments = c.rangeAssign(g)
		g.state = "stable"
	}

	as := g.assignments[req.MemberID]
	m.assigned = as
	return protocol.SyncGroupResp{Assignments: as}, protocol.ErrNone
}

func (c *Coordinator) rangeAssign(g *groupState) map[string][]protocol.Assignment {
	topicSet := map[string]struct{}{}
	for _, m := range g.members {
		for _, t := range m.topics {
			topicSet[t] = struct{}{}
		}
	}
	topics := make([]string, 0, len(topicSet))
	for t := range topicSet {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	memberIDs := make([]string, 0, len(g.members))
	for id := range g.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	out := make(map[string][]protocol.Assignment)
	for _, mid := range memberIDs {
		out[mid] = nil
	}
	if len(memberIDs) == 0 {
		return out
	}

	for _, topic := range topics {
		n, err := c.partitionCounts(topic)
		if err != nil || n <= 0 {
			continue
		}
		for p := int32(0); p < n; p++ {
			mid := memberIDs[int(p)%len(memberIDs)]
			out[mid] = append(out[mid], protocol.Assignment{Topic: topic, Partition: p})
		}
	}
	return out
}

func (c *Coordinator) Heartbeat(req protocol.HeartbeatReq) uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[req.GroupID]
	if !ok {
		return protocol.ErrUnknownMember
	}
	m, ok := g.members[req.MemberID]
	if !ok {
		return protocol.ErrUnknownMember
	}
	if req.GenerationID != g.generation {
		return protocol.ErrRebalanceInProg
	}
	m.lastBeat = time.Now()
	return protocol.ErrNone
}

func (c *Coordinator) Leave(req protocol.LeaveGroupReq) uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[req.GroupID]
	if !ok {
		return protocol.ErrNone
	}
	delete(g.members, req.MemberID)
	g.state = "joining"
	g.generation++
	if g.leaderID == req.MemberID {
		g.leaderID = ""
		for id := range g.members {
			g.leaderID = id
			break
		}
	}
	return protocol.ErrNone
}

func (c *Coordinator) Commit(req protocol.CommitOffsetReq) uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.offsets[req.GroupID]; !ok {
		c.offsets[req.GroupID] = make(map[string]map[int32]int64)
	}
	if _, ok := c.offsets[req.GroupID][req.Topic]; !ok {
		c.offsets[req.GroupID][req.Topic] = make(map[int32]int64)
	}
	c.offsets[req.GroupID][req.Topic][req.Partition] = req.Offset
	if c.store != nil {
		if err := c.store.save(c.offsets); err != nil {
			return protocol.ErrUnknown
		}
	}
	return protocol.ErrNone
}

func (c *Coordinator) FetchOffsets(groupID, topic string) protocol.FetchOffsetsResp {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out protocol.FetchOffsetsResp
	topics := c.offsets[groupID]
	if topics == nil {
		return out
	}
	parts := topics[topic]
	for p, off := range parts {
		out.Offsets = append(out.Offsets, protocol.OffsetEntry{Partition: p, Offset: off})
	}
	sort.Slice(out.Offsets, func(i, j int) bool {
		return out.Offsets[i].Partition < out.Offsets[j].Partition
	})
	return out
}
