package snapshot

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.etcd.io/raft/v3/raftpb"
)

/*
Snapshotter manages snapshot files on disk.
Files named: {term}-{index}.snap
*/
type Snapshotter struct {
	dir string
}

// NewSnapshotter creates a Snapshotter for the given directory
func NewSnapshotter(dir string) (*Snapshotter, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("snap: mkdir: %w", err)
	}
	return &Snapshotter{dir: dir}, nil
}

func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if snapshot.Metadata.Index == 0 {
		return nil
	}

	fileName := fmt.Sprintf("%016x-%016x.snap", snapshot.Metadata.Term, snapshot.Metadata.Index)
	path := filepath.Join(s.dir, fileName)

	data, err := snapshot.Marshal()
	if err != nil {
		return fmt.Errorf("snap: marshal: %w", err)
	}

	temp := path + ".tmp"
	f, err := os.Create(temp)
	if err != nil {
		return fmt.Errorf("snap: create temp: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(temp)
		return fmt.Errorf("snap: write: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(temp)
		return fmt.Errorf("snap: sync: %w", err)
	}
	f.Close()

	return os.Rename(temp, path)
}

// Load return s the most recent snapshot, or nil if none exists
func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}

	if len(names) == 0 {
		return nil, nil
	}

	path := filepath.Join(s.dir, names[len(names)-1])
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("snap: open: %w", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("snap: read: %w", err)
	}

	var snap raftpb.Snapshot
	if err := snap.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("snap: unmarshal: %w", err)
	}

	return &snap, nil
}

func (s *Snapshotter) snapNames() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".snap") {
			names = append(names, e.Name())
		}
	}

	sort.Strings(names)

	return names, nil
}

func (s *Snapshotter) Purge(keep int) {
	names, err := s.snapNames()
	if err != nil || len(names) <= keep {
		return
	}
	for _, name := range names[:len(names)-keep] {
		os.Remove(filepath.Join(s.dir, name))
	}
}
