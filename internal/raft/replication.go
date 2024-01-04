package raft

import (
	"context"
	"log"
	"sort"
	"time"

	pb "github.com/nikhilghind/raft-kv/proto"
)

// sendHeartbeats sends AppendEntries to all peers (heartbeats or with new entries).
func (rn *RaftNode) sendHeartbeats() {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return
	}
	currentTerm := rn.currentTerm
	leaderID := rn.id
	rn.mu.Unlock()

	for peerID, peerAddr := range rn.peers {
		go rn.replicateToFollower(peerID, peerAddr, currentTerm, leaderID)
	}
}

// replicateToFollower sends AppendEntries to a single follower.
func (rn *RaftNode) replicateToFollower(peerID, peerAddr string, currentTerm uint64, leaderID string) {
	rn.mu.Lock()
	if rn.state != Leader || rn.currentTerm != currentTerm {
		rn.mu.Unlock()
		return
	}
	nextIdx := rn.nextIndex[peerID]
	prevLogIndex := nextIdx - 1
	prevLogTerm := rn.log.TermAt(prevLogIndex)
	entries := rn.log.GetEntriesFrom(nextIdx)
	leaderCommit := rn.commitIndex
	rn.mu.Unlock()

	// Convert entries to proto.
	pbEntries := make([]*pb.LogEntry, len(entries))
	for i, e := range entries {
		pbEntries[i] = &pb.LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	conn, err := rn.getPeerConn(peerAddr)
	if err != nil {
		return
	}

	client := pb.NewRaftConsensusClient(conn)
	resp, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         currentTerm,
		LeaderId:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// If we're no longer leader or term changed, ignore.
	if rn.state != Leader || rn.currentTerm != currentTerm {
		return
	}

	if resp.Term > rn.currentTerm {
		rn.currentTerm = resp.Term
		rn.state = Follower
		rn.votedFor = ""
		rn.leaderID = ""
		rn.persistState()
		return
	}

	if resp.Success {
		// Update nextIndex and matchIndex.
		if len(entries) > 0 {
			newMatchIndex := entries[len(entries)-1].Index
			if newMatchIndex > rn.matchIndex[peerID] {
				rn.matchIndex[peerID] = newMatchIndex
			}
			rn.nextIndex[peerID] = newMatchIndex + 1
		}
		rn.advanceCommitIndex()
	} else {
		// Log inconsistency: decrement nextIndex and retry.
		if rn.nextIndex[peerID] > 1 {
			rn.nextIndex[peerID]--
		}
	}
}

// advanceCommitIndex finds the highest N such that a majority of matchIndex[i] >= N
// and log[N].term == currentTerm, then sets commitIndex = N.
// Must be called with rn.mu held.
func (rn *RaftNode) advanceCommitIndex() {
	// Collect all matchIndex values (including leader's own last index).
	matchIndices := make([]uint64, 0, len(rn.peers)+1)
	matchIndices = append(matchIndices, rn.log.LastIndex()) // leader
	for _, idx := range rn.matchIndex {
		matchIndices = append(matchIndices, idx)
	}

	sort.Slice(matchIndices, func(i, j int) bool {
		return matchIndices[i] > matchIndices[j] // descending
	})

	// The median (majority position) is the highest N replicated on a majority.
	majority := (len(matchIndices) / 2) + 1
	if majority > len(matchIndices) {
		return
	}
	n := matchIndices[majority-1]

	// Only advance if the entry at N is from the current term (Raft safety).
	if n > rn.commitIndex && rn.log.TermAt(n) == rn.currentTerm {
		oldCommit := rn.commitIndex
		rn.commitIndex = n
		log.Printf("[%s] advanced commitIndex from %d to %d", rn.id, oldCommit, n)
		rn.applyCommitted()
	}
}

// handleAppendEntries processes an incoming AppendEntries RPC.
func (rn *RaftNode) handleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &pb.AppendEntriesResponse{
		Term:    rn.currentTerm,
		Success: false,
	}

	// Reject if term is behind.
	if req.Term < rn.currentTerm {
		return resp
	}

	// If we see a higher or equal term from a leader, accept authority.
	if req.Term >= rn.currentTerm {
		if req.Term > rn.currentTerm {
			rn.currentTerm = req.Term
			rn.votedFor = ""
			rn.persistState()
		}
		rn.state = Follower
		rn.leaderID = req.LeaderId
	}
	rn.resetElectionTimer()

	resp.Term = rn.currentTerm

	// Check log consistency.
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > rn.log.LastIndex() {
			// We don't have an entry at prevLogIndex.
			return resp
		}
		if rn.log.TermAt(req.PrevLogIndex) != req.PrevLogTerm {
			// Term mismatch — truncate from here.
			rn.log.TruncateFrom(req.PrevLogIndex)
			return resp
		}
	}

	// Append new entries.
	for _, pbEntry := range req.Entries {
		entry := LogEntry{
			Term:    pbEntry.Term,
			Index:   pbEntry.Index,
			Command: pbEntry.Command,
		}

		// If an existing entry conflicts, truncate and append.
		if entry.Index <= rn.log.LastIndex() {
			existing, _ := rn.log.GetEntry(entry.Index)
			if existing.Term != entry.Term {
				rn.log.TruncateFrom(entry.Index)
				if err := rn.log.Append(entry); err != nil {
					log.Printf("[%s] failed to append entry: %v", rn.id, err)
					return resp
				}
			}
			// If terms match, entry already exists — skip.
		} else {
			if err := rn.log.Append(entry); err != nil {
				log.Printf("[%s] failed to append entry: %v", rn.id, err)
				return resp
			}
		}
	}

	// Update commit index.
	if req.LeaderCommit > rn.commitIndex {
		lastNew := rn.log.LastIndex()
		if req.LeaderCommit < lastNew {
			rn.commitIndex = req.LeaderCommit
		} else {
			rn.commitIndex = lastNew
		}
		rn.applyCommitted()
	}

	resp.Success = true
	resp.MatchIndex = rn.log.LastIndex()
	return resp
}
