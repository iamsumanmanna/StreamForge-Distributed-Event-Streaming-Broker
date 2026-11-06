package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/distributed-system/streamforge/internal/transport/protocol"
)

var (
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrPartitionClosed  = errors.New("partition closed")
)

// Options controls durability and retention.
type Options struct {
	SegmentMaxBytes int64
	FsyncEveryN     int
	FsyncInterval   time.Duration
	RetentionBytes  int64
	RetentionAge    time.Duration
}

func DefaultOptions() Options {
	return Options{
		SegmentMaxBytes: 64 << 20,
		FsyncEveryN:     1000,
		FsyncInterval:   50 * time.Millisecond,
		RetentionBytes:  1 << 30,
		RetentionAge:    7 * 24 * time.Hour,
	}
}

// Partition is an ordered append-only log of segments.
type Partition struct {
	mu            sync.RWMutex
	dir           string
	opts          Options
	segments      []*segment
	active        *segment
	nextOffset    int64
	hwm           int64 // high-water mark (committed)
	appendsSince  int
	closed        bool
	stopFsync     chan struct{}
	fsyncDone     chan struct{}
	lastRetention time.Time
}

// OpenPartition opens or creates a partition directory.
func OpenPartition(dir string, opts Options) (*Partition, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &Partition{
		dir:       dir,
		opts:      opts,
		stopFsync: make(chan struct{}),
		fsyncDone: make(chan struct{}),
	}
	if err := p.load(); err != nil {
		return nil, err
	}
	go p.fsyncLoop()
	return p, nil
}

func (p *Partition) load() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}
	var bases []int64
	seen := map[int64]bool{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		baseStr := strings.TrimSuffix(name, ".log")
		base, err := strconv.ParseInt(baseStr, 10, 64)
		if err != nil {
			continue
		}
		if !seen[base] {
			seen[base] = true
			bases = append(bases, base)
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	if len(bases) == 0 {
		seg, err := openSegment(p.dir, 0, true)
		if err != nil {
			return err
		}
		p.segments = []*segment{seg}
		p.active = seg
		p.nextOffset = 0
		p.hwm = 0
		return nil
	}
	for _, b := range bases {
		seg, err := openSegment(p.dir, b, false)
		if err != nil {
			return err
		}
		p.segments = append(p.segments, seg)
	}
	p.active = p.segments[len(p.segments)-1]
	p.nextOffset = p.active.nextOffset
	p.hwm = p.nextOffset
	return nil
}

func (p *Partition) fsyncLoop() {
	defer close(p.fsyncDone)
	t := time.NewTicker(p.opts.FsyncInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stopFsync:
			_ = p.syncActive()
			return
		case <-t.C:
			_ = p.syncActive()
			p.maybeRetain()
		}
	}
}

func (p *Partition) syncActive() error {
	p.mu.RLock()
	active := p.active
	p.mu.RUnlock()
	if active == nil {
		return nil
	}
	return active.sync()
}

// Append appends a record and advances HWM (local durable path / tests).
func (p *Partition) Append(key, value []byte) (int64, error) {
	off, err := p.AppendWithTime(time.Now().UnixMilli(), 0, key, value)
	if err != nil {
		return 0, err
	}
	p.SetHighWatermark(p.NextOffset())
	return off, nil
}

func (p *Partition) AppendWithTime(ts int64, attrs uint8, key, value []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, ErrPartitionClosed
	}
	if err := p.rollIfNeededLocked(); err != nil {
		return 0, err
	}
	off, err := p.active.append(ts, attrs, key, value)
	if err != nil {
		return 0, err
	}
	p.nextOffset = off + 1
	p.appendsSince++
	if p.opts.FsyncEveryN > 0 && p.appendsSince >= p.opts.FsyncEveryN {
		_ = p.active.sync()
		p.appendsSince = 0
	}
	return off, nil
}

// AppendAt is used by followers to append a leader-assigned offset.
func (p *Partition) AppendAt(offset int64, ts int64, attrs uint8, key, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPartitionClosed
	}
	if offset < p.nextOffset {
		return nil // already have it
	}
	if offset > p.nextOffset {
		return fmt.Errorf("gap: next=%d got=%d", p.nextOffset, offset)
	}
	if err := p.rollIfNeededLocked(); err != nil {
		return err
	}
	if err := p.active.appendAt(offset, ts, attrs, key, value); err != nil {
		return err
	}
	p.nextOffset = offset + 1
	return nil
}

func (p *Partition) rollIfNeededLocked() error {
	if p.active.size < p.opts.SegmentMaxBytes {
		return nil
	}
	_ = p.active.sync()
	base := p.nextOffset
	seg, err := openSegment(p.dir, base, true)
	if err != nil {
		return err
	}
	p.segments = append(p.segments, seg)
	p.active = seg
	return nil
}

// Read reads up to maxBytes of records starting at offset (inclusive).
func (p *Partition) Read(offset int64, maxBytes int) ([]protocol.Record, int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, 0, ErrPartitionClosed
	}
	hwm := p.hwm
	if offset >= hwm {
		return nil, hwm, nil
	}
	if offset < p.baseOffsetLocked() {
		return nil, hwm, ErrOffsetOutOfRange
	}
	var out []protocol.Record
	bytesUsed := 0
	cur := offset
	for cur < hwm && bytesUsed < maxBytes {
		seg := p.findSegmentLocked(cur)
		if seg == nil {
			break
		}
		remain := maxBytes - bytesUsed
		if remain <= 0 {
			break
		}
		recs, err := seg.readFrom(cur, remain)
		if err != nil {
			return nil, hwm, err
		}
		if len(recs) == 0 {
			// Advance to next segment if current has no more readable data
			nextBase := seg.nextOffset
			if nextBase <= cur {
				break
			}
			cur = nextBase
			continue
		}
		for _, r := range recs {
			if r.Offset >= hwm {
				break
			}
			out = append(out, r)
			// approximate size
			bytesUsed += 24 + len(r.Key) + len(r.Value)
			cur = r.Offset + 1
			if bytesUsed >= maxBytes {
				break
			}
		}
	}
	return out, hwm, nil
}

func (p *Partition) baseOffsetLocked() int64 {
	if len(p.segments) == 0 {
		return 0
	}
	return p.segments[0].baseOffset
}

func (p *Partition) findSegmentLocked(offset int64) *segment {
	// last segment with baseOffset <= offset
	var found *segment
	for _, s := range p.segments {
		if s.baseOffset <= offset {
			found = s
		} else {
			break
		}
	}
	return found
}

func (p *Partition) NextOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextOffset
}

func (p *Partition) HighWatermark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.hwm
}

func (p *Partition) SetHighWatermark(hwm int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if hwm > p.hwm && hwm <= p.nextOffset {
		p.hwm = hwm
	}
}

func (p *Partition) maybeRetain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.lastRetention) < time.Minute {
		return
	}
	p.lastRetention = time.Now()
	if len(p.segments) <= 1 {
		return
	}
	var total int64
	for _, s := range p.segments {
		total += s.size
	}
	cutoffTime := time.Now().Add(-p.opts.RetentionAge).UnixMilli()
	for len(p.segments) > 1 {
		oldest := p.segments[0]
		dropBySize := p.opts.RetentionBytes > 0 && total > p.opts.RetentionBytes
		dropByAge := false
		if p.opts.RetentionAge > 0 && oldest.nextOffset > oldest.baseOffset {
			// approximate: if segment fully before cutoff by checking file mtime
			lp, _ := segmentPaths(oldest.dir, oldest.baseOffset)
			if info, err := os.Stat(lp); err == nil {
				if info.ModTime().UnixMilli() < cutoffTime {
					dropByAge = true
				}
			}
		}
		if !dropBySize && !dropByAge {
			break
		}
		total -= oldest.size
		_ = oldest.remove()
		p.segments = p.segments[1:]
	}
}

func (p *Partition) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.stopFsync)
	p.mu.Unlock()
	<-p.fsyncDone
	p.mu.Lock()
	defer p.mu.Unlock()
	var err error
	for _, s := range p.segments {
		if e := s.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (p *Partition) Dir() string { return p.dir }

// TopicManager manages topics and partitions on local disk.
type TopicManager struct {
	mu   sync.RWMutex
	root string
	opts Options
	// topic -> partitionID -> *Partition
	parts map[string]map[int32]*Partition
}

func NewTopicManager(root string, opts Options) (*TopicManager, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	tm := &TopicManager{
		root:  root,
		opts:  opts,
		parts: make(map[string]map[int32]*Partition),
	}
	if err := tm.recover(); err != nil {
		return nil, err
	}
	return tm, nil
}

func (tm *TopicManager) recover() error {
	entries, err := os.ReadDir(tm.root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		topic := e.Name()
		pdir := filepath.Join(tm.root, topic)
		parts, err := os.ReadDir(pdir)
		if err != nil {
			return err
		}
		tm.parts[topic] = make(map[int32]*Partition)
		for _, pe := range parts {
			if !pe.IsDir() || !strings.HasPrefix(pe.Name(), "partition-") {
				continue
			}
			idStr := strings.TrimPrefix(pe.Name(), "partition-")
			id64, err := strconv.ParseInt(idStr, 10, 32)
			if err != nil {
				continue
			}
			part, err := OpenPartition(filepath.Join(pdir, pe.Name()), tm.opts)
			if err != nil {
				return err
			}
			tm.parts[topic][int32(id64)] = part
		}
	}
	return nil
}

func (tm *TopicManager) CreateTopic(name string, partitions int32) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if _, ok := tm.parts[name]; ok {
		return ErrTopicExists
	}
	if partitions <= 0 {
		partitions = 1
	}
	tm.parts[name] = make(map[int32]*Partition)
	for i := int32(0); i < partitions; i++ {
		dir := filepath.Join(tm.root, name, fmt.Sprintf("partition-%d", i))
		part, err := OpenPartition(dir, tm.opts)
		if err != nil {
			return err
		}
		tm.parts[name][i] = part
	}
	return nil
}

// EnsurePartition opens/creates topic partition if missing (used by followers).
func (tm *TopicManager) EnsurePartition(name string, partition int32) (*Partition, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	ps, ok := tm.parts[name]
	if !ok {
		ps = make(map[int32]*Partition)
		tm.parts[name] = ps
	}
	if p, ok := ps[partition]; ok {
		return p, nil
	}
	dir := filepath.Join(tm.root, name, fmt.Sprintf("partition-%d", partition))
	part, err := OpenPartition(dir, tm.opts)
	if err != nil {
		return nil, err
	}
	ps[partition] = part
	return part, nil
}

var ErrTopicExists = errors.New("topic already exists")
var ErrUnknownTopic = errors.New("unknown topic")
var ErrUnknownPartition = errors.New("unknown partition")

func (tm *TopicManager) Get(topic string, partition int32) (*Partition, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	ps, ok := tm.parts[topic]
	if !ok {
		return nil, ErrUnknownTopic
	}
	p, ok := ps[partition]
	if !ok {
		return nil, ErrUnknownPartition
	}
	return p, nil
}

func (tm *TopicManager) PartitionCount(topic string) (int32, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	ps, ok := tm.parts[topic]
	if !ok {
		return 0, ErrUnknownTopic
	}
	return int32(len(ps)), nil
}

func (tm *TopicManager) ListTopics() map[string][]int32 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	out := make(map[string][]int32)
	for t, ps := range tm.parts {
		ids := make([]int32, 0, len(ps))
		for id := range ps {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		out[t] = ids
	}
	return out
}

func (tm *TopicManager) Close() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	var err error
	for _, ps := range tm.parts {
		for _, p := range ps {
			if e := p.Close(); e != nil && err == nil {
				err = e
			}
		}
	}
	return err
}
