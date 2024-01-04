package store

import (
	"encoding/json"
	"fmt"
	"sync"
)

// CommandType represents the type of KV operation.
type CommandType int

const (
	CmdPut    CommandType = 1
	CmdDelete CommandType = 2
)

// Command is a serializable KV operation that gets stored in the Raft log.
type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value string      `json:"value,omitempty"`
}

// EncodeCommand serializes a command to bytes.
func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

// DecodeCommand deserializes a command from bytes.
func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}

// KVStore is a thread-safe in-memory key-value store.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore creates a new empty KV store.
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Put sets a key-value pair.
func (s *KVStore) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves a value by key.
func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Delete removes a key.
func (s *KVStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Apply deserializes and applies a command from the Raft log.
func (s *KVStore) Apply(cmdBytes []byte) error {
	cmd, err := DecodeCommand(cmdBytes)
	if err != nil {
		return fmt.Errorf("decode command: %w", err)
	}
	switch cmd.Type {
	case CmdPut:
		s.Put(cmd.Key, cmd.Value)
	case CmdDelete:
		s.Delete(cmd.Key)
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
	return nil
}
