package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nikhilghind/raft-kv/internal/config"
	"github.com/nikhilghind/raft-kv/internal/store"
	pb "github.com/nikhilghind/raft-kv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RaftNode is the core Raft state machine.
type RaftNode struct {
	mu sync.Mutex

	// Identity and config
	id     string
	peers  map[string]string // peerID -> addr
	config *config.Config

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         *RaftLog

	// Volatile state
	state       NodeState
	leaderID    string
	commitIndex uint64
	lastApplied uint64

	// Leader-only volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Application state machine
	kvStore *store.KVStore

	// gRPC connections to peers (cached)
	peerConns   map[string]*grpc.ClientConn
	peerConnsMu sync.Mutex

	// Channel for notifying waiters that an entry was committed.
	commitCh    chan struct{}
	commitWaiters   map[uint64]chan struct{} // index -> notify channel
	commitWaitersMu sync.Mutex

	// Data directory for persistence
	dataDir string
}

// NewRaftNode creates and initializes a new Raft node as a Follower.
func NewRaftNode(cfg *config.Config, kvStore *store.KVStore) (*RaftNode, error) {
	rl, err := NewRaftLog(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("create raft log: %w", err)
	}

	rn := &RaftNode{
		id:            cfg.NodeID,
		peers:         cfg.Peers,
		config:        cfg,
		log:           rl,
		state:         Follower,
		kvStore:       kvStore,
		peerConns:     make(map[string]*grpc.ClientConn),
		commitCh:      make(chan struct{}, 256),
		commitWaiters: make(map[uint64]chan struct{}),
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		dataDir:       cfg.DataDir,
	}

	// Load persistent state from disk.
	rn.loadState()

	return rn, nil
}

// Run starts the main Raft loop. Blocks until ctx is cancelled.
func (rn *RaftNode) Run(ctx context.Context) {
	rn.mu.Lock()
	rn.electionTimer = time.NewTimer(rn.randomElectionTimeout())
	rn.heartbeatTimer = time.NewTimer(time.Duration(rn.config.HeartbeatIntervalMs) * time.Millisecond)
	rn.heartbeatTimer.Stop() // Only leaders send heartbeats.
	rn.mu.Unlock()

	log.Printf("[%s] Raft node started as %s (term %d)", rn.id, rn.state, rn.currentTerm)

	for {
		select {
		case <-ctx.Done():
			rn.log.Close()
			return
		case <-rn.electionTimer.C:
			rn.mu.Lock()
			currentState := rn.state
			rn.mu.Unlock()

			if currentState != Leader {
				rn.startElection()
			}
		case <-rn.heartbeatTimer.C:
			rn.mu.Lock()
			isLeader := rn.state == Leader
			rn.mu.Unlock()

			if isLeader {
				rn.sendHeartbeats()
				rn.heartbeatTimer.Reset(time.Duration(rn.config.HeartbeatIntervalMs) * time.Millisecond)
			}
		}
	}
}

// Propose proposes a new command. Only succeeds on the leader.
// Blocks until the entry is committed or ctx expires.
func (rn *RaftNode) Propose(ctx context.Context, command []byte) error {
	rn.mu.Lock()
	if rn.state != Leader {
		leaderID := rn.leaderID
		rn.mu.Unlock()
		if leaderID != "" {
			return fmt.Errorf("not leader; leader is %s", leaderID)
		}
		return fmt.Errorf("not leader; no known leader")
	}

	entry := LogEntry{
		Term:    rn.currentTerm,
		Index:   rn.log.LastIndex() + 1,
		Command: command,
	}

	if err := rn.log.Append(entry); err != nil {
		rn.mu.Unlock()
		return fmt.Errorf("append log: %w", err)
	}

	index := entry.Index

	// Create a waiter for this index.
	ch := make(chan struct{}, 1)
	rn.commitWaitersMu.Lock()
	rn.commitWaiters[index] = ch
	rn.commitWaitersMu.Unlock()

	rn.mu.Unlock()

	// Trigger immediate replication.
	rn.sendHeartbeats()

	// Wait for commit.
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		// Clean up waiter.
		rn.commitWaitersMu.Lock()
		delete(rn.commitWaiters, index)
		rn.commitWaitersMu.Unlock()
		return ctx.Err()
	}
}

// applyCommitted applies all committed but unapplied entries to the KV store.
// Must be called with rn.mu held.
func (rn *RaftNode) applyCommitted() {
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry, ok := rn.log.GetEntry(rn.lastApplied)
		if !ok {
			log.Printf("[%s] BUG: missing log entry at index %d", rn.id, rn.lastApplied)
			continue
		}
		if len(entry.Command) > 0 {
			if err := rn.kvStore.Apply(entry.Command); err != nil {
				log.Printf("[%s] failed to apply entry %d: %v", rn.id, rn.lastApplied, err)
			}
		}

		// Notify any waiter for this index.
		rn.commitWaitersMu.Lock()
		if ch, ok := rn.commitWaiters[rn.lastApplied]; ok {
			close(ch)
			delete(rn.commitWaiters, rn.lastApplied)
		}
		rn.commitWaitersMu.Unlock()
	}
}

// becomeLeader transitions to Leader state. Must be called with rn.mu held.
func (rn *RaftNode) becomeLeader() {
	log.Printf("[%s] became LEADER for term %d", rn.id, rn.currentTerm)
	rn.state = Leader
	rn.leaderID = rn.id

	// Initialize nextIndex and matchIndex for all peers.
	lastIndex := rn.log.LastIndex()
	for peerID := range rn.peers {
		rn.nextIndex[peerID] = lastIndex + 1
		rn.matchIndex[peerID] = 0
	}

	// Stop election timer, start heartbeat timer.
	rn.electionTimer.Stop()
	rn.heartbeatTimer.Reset(time.Duration(rn.config.HeartbeatIntervalMs) * time.Millisecond)
}

// resetElectionTimer resets the election timer with a random timeout.
// Can be called with or without lock — timer operations are safe.
func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Reset(rn.randomElectionTimeout())
	}
}

func (rn *RaftNode) randomElectionTimeout() time.Duration {
	min := rn.config.ElectionTimeoutMin
	max := rn.config.ElectionTimeoutMax
	ms := min + rand.Intn(max-min)
	return time.Duration(ms) * time.Millisecond
}

// getPeerConn returns a cached gRPC connection to a peer.
func (rn *RaftNode) getPeerConn(addr string) (*grpc.ClientConn, error) {
	rn.peerConnsMu.Lock()
	defer rn.peerConnsMu.Unlock()

	if conn, ok := rn.peerConns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	rn.peerConns[addr] = conn
	return conn, nil
}

// persistState saves currentTerm and votedFor to disk.
func (rn *RaftNode) persistState() {
	state := PersistentState{
		CurrentTerm: rn.currentTerm,
		VotedFor:    rn.votedFor,
	}
	data, err := json.Marshal(state)
	if err != nil {
		log.Printf("[%s] failed to marshal state: %v", rn.id, err)
		return
	}
	path := filepath.Join(rn.dataDir, "state.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		log.Printf("[%s] failed to persist state: %v", rn.id, err)
	}
}

// loadState restores currentTerm and votedFor from disk.
func (rn *RaftNode) loadState() {
	path := filepath.Join(rn.dataDir, "state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return // No state file, start fresh.
	}
	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("[%s] failed to unmarshal state: %v", rn.id, err)
		return
	}
	rn.currentTerm = state.CurrentTerm
	rn.votedFor = state.VotedFor
	log.Printf("[%s] restored state: term=%d, votedFor=%s", rn.id, rn.currentTerm, rn.votedFor)
}

// State returns the current node state.
func (rn *RaftNode) State() NodeState {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state
}

// LeaderID returns the current known leader ID.
func (rn *RaftNode) LeaderID() string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.leaderID
}

// LeaderAddr returns the address of the current leader, if known.
func (rn *RaftNode) LeaderAddr() string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.leaderID == "" {
		return ""
	}
	if rn.leaderID == rn.id {
		return rn.config.ListenAddr
	}
	return rn.peers[rn.leaderID]
}

// KVStore returns the underlying KV store (for reads).
func (rn *RaftNode) KVStore() *store.KVStore {
	return rn.kvStore
}

// HandleRequestVote is the exported handler for RequestVote RPCs.
func (rn *RaftNode) HandleRequestVote(req *pb.VoteRequest) *pb.VoteResponse {
	return rn.handleRequestVote(req)
}

// HandleAppendEntries is the exported handler for AppendEntries RPCs.
func (rn *RaftNode) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	return rn.handleAppendEntries(req)
}
