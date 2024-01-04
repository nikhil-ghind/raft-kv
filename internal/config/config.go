package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds all configuration for a Raft node.
type Config struct {
	NodeID              string
	ListenAddr          string
	Peers               map[string]string // id -> addr
	DataDir             string
	ElectionTimeoutMin  int // milliseconds
	ElectionTimeoutMax  int // milliseconds
	HeartbeatIntervalMs int
}

// DefaultConfig returns a config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		NodeID:              "node1",
		ListenAddr:          "0.0.0.0:50051",
		Peers:               make(map[string]string),
		DataDir:             "/tmp/raft-kv",
		ElectionTimeoutMin:  150,
		ElectionTimeoutMax:  300,
		HeartbeatIntervalMs: 50,
	}
}

// LoadFromEnv overrides config values from environment variables.
// RAFT_NODE_ID, RAFT_LISTEN_ADDR, RAFT_PEERS (comma-separated id=addr pairs),
// RAFT_DATA_DIR, RAFT_ELECTION_TIMEOUT_MIN, RAFT_ELECTION_TIMEOUT_MAX, RAFT_HEARTBEAT_INTERVAL
func (c *Config) LoadFromEnv() {
	if v := os.Getenv("RAFT_NODE_ID"); v != "" {
		c.NodeID = v
	}
	if v := os.Getenv("RAFT_LISTEN_ADDR"); v != "" {
		c.ListenAddr = v
	}
	if v := os.Getenv("RAFT_PEERS"); v != "" {
		c.Peers = parsePeers(v)
	}
	if v := os.Getenv("RAFT_DATA_DIR"); v != "" {
		c.DataDir = v
	}
	if v := os.Getenv("RAFT_ELECTION_TIMEOUT_MIN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.ElectionTimeoutMin = n
		}
	}
	if v := os.Getenv("RAFT_ELECTION_TIMEOUT_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.ElectionTimeoutMax = n
		}
	}
	if v := os.Getenv("RAFT_HEARTBEAT_INTERVAL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.HeartbeatIntervalMs = n
		}
	}
}

// parsePeers parses "id1=addr1,id2=addr2" format.
func parsePeers(s string) map[string]string {
	peers := make(map[string]string)
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			peers[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return peers
}

// Validate checks the config for obvious errors.
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if c.ListenAddr == "" {
		return fmt.Errorf("listen address is required")
	}
	if c.ElectionTimeoutMin >= c.ElectionTimeoutMax {
		return fmt.Errorf("election timeout min must be less than max")
	}
	if c.HeartbeatIntervalMs <= 0 {
		return fmt.Errorf("heartbeat interval must be positive")
	}
	return nil
}
