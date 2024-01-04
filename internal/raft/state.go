package raft

import "fmt"

// NodeState represents the current role of a Raft node.
type NodeState int

const (
	Follower  NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("Unknown(%d)", int(s))
	}
}

// PersistentState holds the state that must survive restarts.
type PersistentState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"` // empty string means no vote
}
