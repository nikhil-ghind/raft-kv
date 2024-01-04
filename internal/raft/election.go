package raft

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/nikhilghind/raft-kv/proto"
)

// startElection transitions to Candidate and runs an election.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	currentTerm := rn.currentTerm
	lastLogIndex := rn.log.LastIndex()
	lastLogTerm := rn.log.LastTerm()
	rn.persistState()
	rn.resetElectionTimer()
	rn.mu.Unlock()

	log.Printf("[%s] starting election for term %d", rn.id, currentTerm)

	var votesReceived atomic.Int32
	votesReceived.Store(1) // vote for self
	totalNodes := len(rn.peers) + 1

	var wg sync.WaitGroup
	for peerID, peerAddr := range rn.peers {
		wg.Add(1)
		go func(pid, addr string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			conn, err := rn.getPeerConn(addr)
			if err != nil {
				log.Printf("[%s] failed to connect to %s: %v", rn.id, pid, err)
				return
			}

			client := pb.NewRaftConsensusClient(conn)
			resp, err := client.RequestVote(ctx, &pb.VoteRequest{
				Term:         currentTerm,
				CandidateId:  rn.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				log.Printf("[%s] RequestVote to %s failed: %v", rn.id, pid, err)
				return
			}

			// If we discover a higher term, revert to follower.
			if resp.Term > currentTerm {
				rn.mu.Lock()
				if resp.Term > rn.currentTerm {
					rn.currentTerm = resp.Term
					rn.state = Follower
					rn.votedFor = ""
					rn.leaderID = ""
					rn.persistState()
				}
				rn.mu.Unlock()
				return
			}

			if resp.VoteGranted {
				newCount := votesReceived.Add(1)
				if int(newCount) >= (totalNodes/2)+1 {
					rn.mu.Lock()
					// Only become leader if still candidate for this term.
					if rn.state == Candidate && rn.currentTerm == currentTerm {
						rn.becomeLeader()
					}
					rn.mu.Unlock()
				}
			}
		}(peerID, peerAddr)
	}

	// Don't block — let votes trickle in. If majority arrives, we become leader
	// in the goroutine above. If election times out, the main loop will restart.
	go func() {
		wg.Wait()
		// If we didn't get enough votes, we'll time out and retry.
		rn.mu.Lock()
		if rn.state == Candidate && rn.currentTerm == currentTerm {
			log.Printf("[%s] election for term %d: got %d/%d votes (need %d)",
				rn.id, currentTerm, votesReceived.Load(), totalNodes, (totalNodes/2)+1)
		}
		rn.mu.Unlock()
	}()
}

// handleRequestVote processes an incoming RequestVote RPC.
func (rn *RaftNode) handleRequestVote(req *pb.VoteRequest) *pb.VoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &pb.VoteResponse{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	// If the request term is behind, reject.
	if req.Term < rn.currentTerm {
		return resp
	}

	// If we see a higher term, step down.
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.state = Follower
		rn.votedFor = ""
		rn.leaderID = ""
		rn.persistState()
	}

	resp.Term = rn.currentTerm

	// Grant vote if we haven't voted for someone else in this term,
	// and the candidate's log is at least as up-to-date as ours.
	if (rn.votedFor == "" || rn.votedFor == req.CandidateId) &&
		rn.isLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		rn.votedFor = req.CandidateId
		rn.persistState()
		rn.resetElectionTimer()
		resp.VoteGranted = true
		log.Printf("[%s] granted vote to %s for term %d", rn.id, req.CandidateId, req.Term)
	}

	return resp
}

// isLogUpToDate returns true if the candidate's log is at least as up-to-date.
// Must be called with rn.mu held.
func (rn *RaftNode) isLogUpToDate(candidateLastIndex, candidateLastTerm uint64) bool {
	myLastTerm := rn.log.LastTerm()
	myLastIndex := rn.log.LastIndex()

	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIndex >= myLastIndex
}
