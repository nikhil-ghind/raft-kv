// Hand-coded gRPC service definitions using JSON codec.
// In production, use protoc-generated code with protobuf.

package proto

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

// jsonCodec is a gRPC codec that uses JSON serialization.
type jsonCodec struct{}

func (jsonCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (jsonCodec) Name() string {
	return "proto" // Override default proto codec name so gRPC uses this.
}

func (jsonCodec) String() string {
	return "json"
}

// =====================
// RaftKV Service
// =====================

type RaftKVServer interface {
	Put(context.Context, *PutRequest) (*PutResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
}

type UnimplementedRaftKVServer struct{}

func (UnimplementedRaftKVServer) Put(context.Context, *PutRequest) (*PutResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}
func (UnimplementedRaftKVServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}
func (UnimplementedRaftKVServer) Delete(context.Context, *DeleteRequest) (*DeleteResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func RegisterRaftKVServer(s *grpc.Server, srv RaftKVServer) {
	s.RegisterService(&_RaftKV_serviceDesc, srv)
}

var _RaftKV_serviceDesc = grpc.ServiceDesc{
	ServiceName: "raftkv.RaftKV",
	HandlerType: (*RaftKVServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Put", Handler: _RaftKV_Put_Handler},
		{MethodName: "Get", Handler: _RaftKV_Get_Handler},
		{MethodName: "Delete", Handler: _RaftKV_Delete_Handler},
	},
	Streams: []grpc.StreamDesc{},
}

func _RaftKV_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, _ grpc.UnaryServerInterceptor) (interface{}, error) {
	req := new(PutRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(RaftKVServer).Put(ctx, req)
}

func _RaftKV_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, _ grpc.UnaryServerInterceptor) (interface{}, error) {
	req := new(GetRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(RaftKVServer).Get(ctx, req)
}

func _RaftKV_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, _ grpc.UnaryServerInterceptor) (interface{}, error) {
	req := new(DeleteRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(RaftKVServer).Delete(ctx, req)
}

// RaftKVClient is the client-side interface.
type RaftKVClient interface {
	Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error)
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error)
}

type raftKVClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftKVClient(cc grpc.ClientConnInterface) RaftKVClient {
	return &raftKVClient{cc}
}

func (c *raftKVClient) Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	out := new(PutResponse)
	err := c.cc.Invoke(ctx, "/raftkv.RaftKV/Put", in, out, opts...)
	return out, err
}

func (c *raftKVClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/raftkv.RaftKV/Get", in, out, opts...)
	return out, err
}

func (c *raftKVClient) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	out := new(DeleteResponse)
	err := c.cc.Invoke(ctx, "/raftkv.RaftKV/Delete", in, out, opts...)
	return out, err
}

// =====================
// RaftConsensus Service
// =====================

type RaftConsensusServer interface {
	RequestVote(context.Context, *VoteRequest) (*VoteResponse, error)
	AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error)
}

type UnimplementedRaftConsensusServer struct{}

func (UnimplementedRaftConsensusServer) RequestVote(context.Context, *VoteRequest) (*VoteResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}
func (UnimplementedRaftConsensusServer) AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func RegisterRaftConsensusServer(s *grpc.Server, srv RaftConsensusServer) {
	s.RegisterService(&_RaftConsensus_serviceDesc, srv)
}

var _RaftConsensus_serviceDesc = grpc.ServiceDesc{
	ServiceName: "raftkv.RaftConsensus",
	HandlerType: (*RaftConsensusServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "RequestVote", Handler: _RaftConsensus_RequestVote_Handler},
		{MethodName: "AppendEntries", Handler: _RaftConsensus_AppendEntries_Handler},
	},
	Streams: []grpc.StreamDesc{},
}

func _RaftConsensus_RequestVote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, _ grpc.UnaryServerInterceptor) (interface{}, error) {
	req := new(VoteRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(RaftConsensusServer).RequestVote(ctx, req)
}

func _RaftConsensus_AppendEntries_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, _ grpc.UnaryServerInterceptor) (interface{}, error) {
	req := new(AppendEntriesRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(RaftConsensusServer).AppendEntries(ctx, req)
}

type RaftConsensusClient interface {
	RequestVote(ctx context.Context, in *VoteRequest, opts ...grpc.CallOption) (*VoteResponse, error)
	AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error)
}

type raftConsensusClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftConsensusClient(cc grpc.ClientConnInterface) RaftConsensusClient {
	return &raftConsensusClient{cc}
}

func (c *raftConsensusClient) RequestVote(ctx context.Context, in *VoteRequest, opts ...grpc.CallOption) (*VoteResponse, error) {
	out := new(VoteResponse)
	err := c.cc.Invoke(ctx, "/raftkv.RaftConsensus/RequestVote", in, out, opts...)
	return out, err
}

func (c *raftConsensusClient) AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error) {
	out := new(AppendEntriesResponse)
	err := c.cc.Invoke(ctx, "/raftkv.RaftConsensus/AppendEntries", in, out, opts...)
	return out, err
}
