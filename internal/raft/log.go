package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command []byte `json:"command"`
}

// RaftLog manages the ordered log of entries with WAL persistence.
type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry // 1-indexed logical; entries[0] is a sentinel
	walFile *os.File
	walPath string
}

// NewRaftLog creates a new log, replaying any existing WAL from dataDir.
func NewRaftLog(dataDir string) (*RaftLog, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	rl := &RaftLog{
		// Sentinel entry at index 0 so real entries are 1-indexed.
		entries: []LogEntry{{Term: 0, Index: 0}},
		walPath: filepath.Join(dataDir, "wal.log"),
	}

	// Replay existing WAL.
	if err := rl.replayWAL(); err != nil {
		return nil, fmt.Errorf("replay WAL: %w", err)
	}

	// Open WAL for appending.
	f, err := os.OpenFile(rl.walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	rl.walFile = f

	return rl, nil
}

func (rl *RaftLog) replayWAL() error {
	f, err := os.Open(rl.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("unmarshal WAL entry: %w", err)
		}
		// Handle truncation: if this entry's index is <= our last index,
		// truncate from that point.
		if entry.Index <= rl.lastIndexNoLock() {
			rl.entries = rl.entries[:entry.Index]
		}
		rl.entries = append(rl.entries, entry)
	}
	return scanner.Err()
}

// Append adds entries to the log and persists them to the WAL.
func (rl *RaftLog) Append(entries ...LogEntry) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	for _, e := range entries {
		// Assign index if not set.
		if e.Index == 0 {
			e.Index = rl.lastIndexNoLock() + 1
		}
		// If this overwrites an existing entry, truncate.
		if e.Index <= rl.lastIndexNoLock() {
			rl.entries = rl.entries[:e.Index]
		}
		rl.entries = append(rl.entries, e)

		// Write to WAL.
		if rl.walFile != nil {
			data, err := json.Marshal(e)
			if err != nil {
				return fmt.Errorf("marshal entry: %w", err)
			}
			if _, err := rl.walFile.Write(append(data, '\n')); err != nil {
				return fmt.Errorf("write WAL: %w", err)
			}
		}
	}

	if rl.walFile != nil {
		if err := rl.walFile.Sync(); err != nil {
			return fmt.Errorf("sync WAL: %w", err)
		}
	}
	return nil
}

// GetEntry returns the entry at the given index. Returns zero entry if out of range.
func (rl *RaftLog) GetEntry(index uint64) (LogEntry, bool) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if index == 0 || index >= uint64(len(rl.entries)) {
		return LogEntry{}, false
	}
	return rl.entries[index], true
}

// GetEntriesFrom returns all entries from startIndex (inclusive) to the end.
func (rl *RaftLog) GetEntriesFrom(startIndex uint64) []LogEntry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if startIndex >= uint64(len(rl.entries)) {
		return nil
	}
	result := make([]LogEntry, len(rl.entries)-int(startIndex))
	copy(result, rl.entries[startIndex:])
	return result
}

// LastIndex returns the index of the last log entry.
func (rl *RaftLog) LastIndex() uint64 {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.lastIndexNoLock()
}

func (rl *RaftLog) lastIndexNoLock() uint64 {
	return uint64(len(rl.entries) - 1)
}

// LastTerm returns the term of the last log entry.
func (rl *RaftLog) LastTerm() uint64 {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.entries[len(rl.entries)-1].Term
}

// TermAt returns the term at the given index.
func (rl *RaftLog) TermAt(index uint64) uint64 {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if index >= uint64(len(rl.entries)) {
		return 0
	}
	return rl.entries[index].Term
}

// TruncateFrom removes all entries from index onwards.
func (rl *RaftLog) TruncateFrom(index uint64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if index < uint64(len(rl.entries)) {
		rl.entries = rl.entries[:index]
	}
}

// Close closes the WAL file.
func (rl *RaftLog) Close() error {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.walFile != nil {
		return rl.walFile.Close()
	}
	return nil
}
