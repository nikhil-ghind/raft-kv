package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/nikhilghind/raft-kv/internal/raft"
	"github.com/nikhilghind/raft-kv/internal/store"
	pb "github.com/nikhilghind/raft-kv/proto"
)

// Server implements both RaftKV and RaftConsensus gRPC services.
type Server struct {
	pb.UnimplementedRaftKVServer
	pb.UnimplementedRaftConsensusServer

	node *raft.RaftNode
}

// NewServer creates a new gRPC server backed by the given Raft node.
func NewServer(node *raft.RaftNode) *Server {
	return &Server{node: node}
}

// --- KV Operations ---

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if s.node.State() != raft.Leader {
		return &pb.PutResponse{
			Success:    false,
			Error:      "not leader",
			LeaderHint: s.node.LeaderAddr(),
		}, nil
	}

	cmd, err := store.EncodeCommand(store.Command{
		Type:  store.CmdPut,
		Key:   req.Key,
		Value: req.Value,
	})
	if err != nil {
		return &pb.PutResponse{Success: false, Error: fmt.Sprintf("encode: %v", err)}, nil
	}

	proposeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := s.node.Propose(proposeCtx, cmd); err != nil {
		return &pb.PutResponse{Success: false, Error: err.Error()}, nil
	}

	return &pb.PutResponse{Success: true}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// Reads can be served by any node for simplicity (eventual consistency).
	// For linearizable reads, we'd need a read index or lease mechanism.
	// Here we allow reads on followers but hint the leader for writes.
	value, found := s.node.KVStore().Get(req.Key)
	return &pb.GetResponse{
		Value:      value,
		Found:      found,
		LeaderHint: s.node.LeaderAddr(),
	}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if s.node.State() != raft.Leader {
		return &pb.DeleteResponse{
			Success:    false,
			Error:      "not leader",
			LeaderHint: s.node.LeaderAddr(),
		}, nil
	}

	cmd, err := store.EncodeCommand(store.Command{
		Type: store.CmdDelete,
		Key:  req.Key,
	})
	if err != nil {
		return &pb.DeleteResponse{Success: false, Error: fmt.Sprintf("encode: %v", err)}, nil
	}

	proposeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := s.node.Propose(proposeCtx, cmd); err != nil {
		return &pb.DeleteResponse{Success: false, Error: err.Error()}, nil
	}

	return &pb.DeleteResponse{Success: true}, nil
}

// --- Raft Consensus RPCs ---

func (s *Server) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	resp := s.node.HandleRequestVote(req)
	return resp, nil
}

func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	resp := s.node.HandleAppendEntries(req)
	return resp, nil
}
