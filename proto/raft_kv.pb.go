// Hand-coded protobuf-compatible message types.
// In production, generate with: protoc --go_out=. --go-grpc_out=. proto/raft_kv.proto

package proto

// --- KV Messages ---

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
	Success    bool
	Error      string
	LeaderHint string
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value      string
	Found      bool
	Error      string
	LeaderHint string
}

type DeleteRequest struct {
	Key string
}

type DeleteResponse struct {
	Success    bool
	Error      string
	LeaderHint string
}

// --- Raft Messages ---

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

type VoteRequest struct {
	Term         uint64
	CandidateId  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderId     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term       uint64
	Success    bool
	MatchIndex uint64
}
