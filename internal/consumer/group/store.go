package group

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// offsetStore persists group -> topic -> partition -> offset as JSON.
type offsetStore struct {
	mu   sync.Mutex
	path string
}

type offsetFile struct {
	Offsets map[string]map[string]map[string]int64 `json:"offsets"` // group -> topic -> partStr -> offset
}

func newOffsetStore(dir string) (*offsetStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &offsetStore{path: filepath.Join(dir, "offsets.json")}, nil
}

func (s *offsetStore) load() (map[string]map[string]map[int32]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]map[string]map[int32]int64)
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, err
	}
	var f offsetFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	for g, topics := range f.Offsets {
		out[g] = make(map[string]map[int32]int64)
		for t, parts := range topics {
			out[g][t] = make(map[int32]int64)
			for ps, off := range parts {
				p64, err := strconv.ParseInt(ps, 10, 32)
				if err != nil {
					continue
				}
				out[g][t][int32(p64)] = off
			}
		}
	}
	return out, nil
}

func (s *offsetStore) save(offsets map[string]map[string]map[int32]int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := offsetFile{Offsets: make(map[string]map[string]map[string]int64)}
	for g, topics := range offsets {
		f.Offsets[g] = make(map[string]map[string]int64)
		for t, parts := range topics {
			f.Offsets[g][t] = make(map[string]int64)
			for p, off := range parts {
				f.Offsets[g][t][strconv.FormatInt(int64(p), 10)] = off
			}
		}
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
