package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/distributed-system/streamforge/internal/transport/protocol"
)

// segment is one on-disk chunk of a partition log.
type segment struct {
	mu         sync.RWMutex
	baseOffset int64
	dir        string
	logFile    *os.File
	indexFile  *os.File
	size       int64
	nextOffset int64
	closed     bool
}

func segmentPaths(dir string, base int64) (logPath, indexPath string) {
	name := fmt.Sprintf("%020d", base)
	return filepath.Join(dir, name+".log"), filepath.Join(dir, name+".index")
}

func openSegment(dir string, base int64, create bool) (*segment, error) {
	logPath, indexPath := segmentPaths(dir, base)
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
	}
	lf, err := os.OpenFile(logPath, flag, 0o644)
	if err != nil {
		return nil, err
	}
	idx, err := os.OpenFile(indexPath, flag, 0o644)
	if err != nil {
		lf.Close()
		return nil, err
	}
	info, err := lf.Stat()
	if err != nil {
		lf.Close()
		idx.Close()
		return nil, err
	}
	s := &segment{
		baseOffset: base,
		dir:        dir,
		logFile:    lf,
		indexFile:  idx,
		size:       info.Size(),
		nextOffset: base,
	}
	if err := s.recover(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *segment) recover() error {
	data, err := os.ReadFile(s.logFile.Name())
	if err != nil {
		return err
	}
	var pos int64
	off := s.baseOffset
	for len(data) > 0 {
		if len(data) < 8 {
			// truncate trailing partial
			break
		}
		total := int(binary.BigEndian.Uint32(data[0:4]))
		if total < 4 || len(data) < 4+total {
			break
		}
		_, _, _, _, consumed, derr := protocol.DecodeRecordDisk(data)
		if derr != nil {
			break
		}
		pos += int64(consumed)
		data = data[consumed:]
		off++
	}
	if err := s.logFile.Truncate(pos); err != nil {
		return err
	}
	s.size = pos
	s.nextOffset = off
	if _, err := s.logFile.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return s.rebuildIndex()
}

func (s *segment) rebuildIndex() error {
	if err := s.indexFile.Truncate(0); err != nil {
		return err
	}
	if _, err := s.indexFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	data, err := os.ReadFile(s.logFile.Name())
	if err != nil {
		return err
	}
	var pos int64
	off := s.baseOffset
	const entryInterval int64 = 16
	for len(data) > 0 {
		_, _, _, _, consumed, derr := protocol.DecodeRecordDisk(data)
		if derr != nil {
			break
		}
		if (off-s.baseOffset)%entryInterval == 0 {
			if err := writeIndexEntry(s.indexFile, uint32(off-s.baseOffset), uint32(pos)); err != nil {
				return err
			}
		}
		pos += int64(consumed)
		data = data[consumed:]
		off++
	}
	_, err = s.logFile.Seek(0, io.SeekEnd)
	return err
}

func writeIndexEntry(f *os.File, relOff, position uint32) error {
	var e [8]byte
	binary.BigEndian.PutUint32(e[0:4], relOff)
	binary.BigEndian.PutUint32(e[4:8], position)
	_, err := f.Write(e[:])
	return err
}

func (s *segment) append(ts int64, attrs uint8, key, value []byte) (offset int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, fmt.Errorf("segment closed")
	}
	rec := protocol.EncodeRecordDisk(ts, attrs, key, value)
	n, err := s.logFile.Write(rec)
	if err != nil {
		return 0, err
	}
	offset = s.nextOffset
	rel := uint32(offset - s.baseOffset)
	if rel%16 == 0 {
		_ = writeIndexEntry(s.indexFile, rel, uint32(s.size))
	}
	s.size += int64(n)
	s.nextOffset++
	return offset, nil
}

func (s *segment) appendAt(offset int64, ts int64, attrs uint8, key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("segment closed")
	}
	if offset != s.nextOffset {
		return fmt.Errorf("offset mismatch: want %d got %d", s.nextOffset, offset)
	}
	rec := protocol.EncodeRecordDisk(ts, attrs, key, value)
	n, err := s.logFile.Write(rec)
	if err != nil {
		return err
	}
	rel := uint32(offset - s.baseOffset)
	if rel%16 == 0 {
		_ = writeIndexEntry(s.indexFile, rel, uint32(s.size))
	}
	s.size += int64(n)
	s.nextOffset++
	return nil
}

func (s *segment) sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	return s.indexFile.Sync()
}

func (s *segment) lookup(offset int64) (filePos int64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if offset < s.baseOffset || offset >= s.nextOffset {
		return 0, ErrOffsetOutOfRange
	}
	rel := uint32(offset - s.baseOffset)
	info, err := s.indexFile.Stat()
	if err != nil {
		return 0, err
	}
	entries := info.Size() / 8
	if entries == 0 {
		return 0, nil
	}
	var bestPos uint32
	lo, hi := int64(0), entries-1
	for lo <= hi {
		mid := (lo + hi) / 2
		var e [8]byte
		if _, err := s.indexFile.ReadAt(e[:], mid*8); err != nil {
			return 0, err
		}
		roff := binary.BigEndian.Uint32(e[0:4])
		pos := binary.BigEndian.Uint32(e[4:8])
		if roff <= rel {
			bestPos = pos
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return int64(bestPos), nil
}

func (s *segment) readFrom(offset int64, maxBytes int) ([]protocol.Record, error) {
	pos, err := s.lookup(offset)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, ErrOffsetOutOfRange
	}

	need := s.size - pos
	if need <= 0 {
		return nil, nil
	}
	readSize := need
	if readSize > int64(maxBytes)+64<<10 {
		readSize = int64(maxBytes) + 64<<10
	}
	buf := make([]byte, readSize)
	n, err := s.logFile.ReadAt(buf, pos)
	if err != nil && err != io.EOF {
		return nil, err
	}
	buf = buf[:n]

	// Recompute start offset at `pos` using index relative offset
	curOff := s.offsetAtPos(pos)
	for len(buf) >= 8 && curOff < offset {
		_, _, _, _, consumed, derr := protocol.DecodeRecordDisk(buf)
		if derr != nil {
			return nil, derr
		}
		buf = buf[consumed:]
		curOff++
	}

	var out []protocol.Record
	bytesUsed := 0
	for len(buf) >= 8 && bytesUsed < maxBytes {
		ts, attrs, key, value, consumed, derr := protocol.DecodeRecordDisk(buf)
		if derr != nil {
			break
		}
		if curOff >= s.nextOffset {
			break
		}
		out = append(out, protocol.Record{
			Offset:    curOff,
			Timestamp: ts,
			Attrs:     attrs,
			Key:       key,
			Value:     value,
		})
		buf = buf[consumed:]
		bytesUsed += consumed
		curOff++
	}
	return out, nil
}

func (s *segment) offsetAtPos(pos int64) int64 {
	info, err := s.indexFile.Stat()
	if err != nil || info.Size() == 0 {
		return s.baseOffset
	}
	entries := info.Size() / 8
	var bestRel uint32
	for i := int64(0); i < entries; i++ {
		var e [8]byte
		if _, err := s.indexFile.ReadAt(e[:], i*8); err != nil {
			break
		}
		roff := binary.BigEndian.Uint32(e[0:4])
		p := binary.BigEndian.Uint32(e[4:8])
		if int64(p) <= pos {
			bestRel = roff
		} else {
			break
		}
	}
	return s.baseOffset + int64(bestRel)
}

func (s *segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	var err error
	if s.logFile != nil {
		err = s.logFile.Close()
	}
	if s.indexFile != nil {
		if e := s.indexFile.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (s *segment) remove() error {
	_ = s.Close()
	lp, ip := segmentPaths(s.dir, s.baseOffset)
	_ = os.Remove(lp)
	return os.Remove(ip)
}
