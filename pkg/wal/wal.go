package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

const (
	recEntry     byte = 0x01
	recHardState byte = 0x02
	recSnapshot  byte = 0x03
)

/*
Raft HeadState
HardState {
    Term:   3
    Vote:   1
    Commit: 42
}
*/

type WAL struct {
	mu  sync.Mutex
	f   *os.File
	dir string
}

func Create(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("wal: mkdr: %w", err)
	}

	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640)
	if err != nil {
		return nil, fmt.Errorf("wal: create: %w", err)
	}

	return &WAL{f: f, dir: dir}, nil
}

func Open(dir string) (*WAL, error) {
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0640)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	return &WAL{f: f, dir: dir}, nil
}

func Exist(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "wal.log"))
	return err == nil
}

// ReadAll replays the entire WAL and returns the last HardState + all Entries
func (w *WAL) ReadAll() (raftpb.HardState, []raftpb.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return raftpb.HardState{}, nil, err
	}

	var hs raftpb.HardState
	var entries []raftpb.Entry

	for {
		rectType, data, err := w.readRecord()
		if err == io.EOF {
			break
		}

		if err != nil {
			return raftpb.HardState{}, nil, fmt.Errorf("wal: corrupt: %w", err)
		}

		switch rectType {

		case recEntry:
			var e raftpb.Entry
			if err := e.Unmarshal(data); err != nil {
				return raftpb.HardState{}, nil, fmt.Errorf("wal: bad entry: %w", err)
			}

			entries = append(entries, e)

		case recHardState:
			if err := hs.Unmarshal(data); err != nil {
				return raftpb.HardState{}, nil, fmt.Errorf("wal: bad hardstate: %w", err)
			}

		case recSnapshot:
			if len(data) >= 8 {
				snapIdx := binary.BigEndian.Uint64(data[0:8])
				filtered := entries[:0]
				for _, e := range entries {
					if e.Index > snapIdx {
						filtered = append(filtered, e)
					}
				}
				entries = filtered
			}
		}
	}

	return hs, entries, nil
}

func (w *WAL) Save(hs raftpb.HardState, entries []raftpb.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if hs.Term > 0 || hs.Vote > 0 || hs.Commit > 0 {
		data, err := hs.Marshal()
		if err != nil {
			return fmt.Errorf("wal: marshal hs: %w", err)
		}
		if err := w.writeRecord(recHardState, data); err != nil {
			return err
		}
	}

	for i := range entries {
		data, err := entries[i].Marshal()
		if err != nil {
			return fmt.Errorf("wal: marshal entry: %w", err)
		}
		if err := w.writeRecord(recEntry, data); err != nil {
			return err
		}
	}

	return w.f.Sync()
}

func (w *WAL) SaveSnapshot(index, term uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[0:8], index)
	binary.BigEndian.PutUint64(data[8:16], term)

	if err := w.writeRecord(recSnapshot, data); err != nil {
		return err
	}

	return w.f.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		return w.f.Close()
	}
	return nil
}

func (w *WAL) writeRecord(recType byte, data []byte) error {
	hdr := make([]byte, 5)
	hdr[0] = recType
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(data)))

	if _, err := w.f.Write(hdr); err != nil {
		return err
	}
	if _, err := w.f.Write(data); err != nil {
		return err
	}

	crc := crc32.NewIEEE()
	crc.Write(hdr[:1])
	crc.Write(data)
	crcBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(crcBuf, crc.Sum32())

	_, err := w.f.Write(crcBuf)

	return err
}

func (w *WAL) readRecord() (byte, []byte, error) {
	hdr := make([]byte, 5)
	if _, err := io.ReadFull(w.f, hdr); err != nil {
		return 0, nil, err
	}

	rectType := hdr[0]
	dataLen := binary.BigEndian.Uint32(hdr[1:5])

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(w.f, data); err != nil {
		return 0, nil, fmt.Errorf("truncated data: %w", err)
	}

	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(w.f, crcBuf); err != nil {
		return 0, nil, fmt.Errorf("truncated data: %w", err)
	}

	expectedChecksum := binary.BigEndian.Uint32(crcBuf)
	crc := crc32.NewIEEE()
	crc.Write(hdr[:1])
	crc.Write(data)

	if actual := crc.Sum32(); actual != expectedChecksum {
		return 0, nil, fmt.Errorf("crc mismatch: $%x != %x", expectedChecksum, actual)
	}

	return rectType, data, nil
}
