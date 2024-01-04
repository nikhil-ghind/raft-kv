package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nikhilghind/raft-kv/internal/config"
	"github.com/nikhilghind/raft-kv/internal/raft"
	"github.com/nikhilghind/raft-kv/internal/rpc"
	"github.com/nikhilghind/raft-kv/internal/store"
	pb "github.com/nikhilghind/raft-kv/proto"

	"google.golang.org/grpc"
)

func main() {
	cfg := config.DefaultConfig()

	flag.StringVar(&cfg.NodeID, "id", cfg.NodeID, "Node ID")
	flag.StringVar(&cfg.ListenAddr, "addr", cfg.ListenAddr, "Listen address (host:port)")
	flag.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Data directory for WAL and state")
	peersFlag := flag.String("peers", "", "Comma-separated peers: id1=addr1,id2=addr2")
	flag.IntVar(&cfg.ElectionTimeoutMin, "election-min", cfg.ElectionTimeoutMin, "Min election timeout (ms)")
	flag.IntVar(&cfg.ElectionTimeoutMax, "election-max", cfg.ElectionTimeoutMax, "Max election timeout (ms)")
	flag.IntVar(&cfg.HeartbeatIntervalMs, "heartbeat", cfg.HeartbeatIntervalMs, "Heartbeat interval (ms)")
	flag.Parse()

	// Override from env vars.
	cfg.LoadFromEnv()

	// Command-line peers override env.
	if *peersFlag != "" {
		cfg.Peers = parsePeers(*peersFlag)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	log.Printf("Starting node %s on %s with %d peers", cfg.NodeID, cfg.ListenAddr, len(cfg.Peers))
	for id, addr := range cfg.Peers {
		log.Printf("  peer %s -> %s", id, addr)
	}

	// Create KV store.
	kvStore := store.NewKVStore()

	// Create Raft node.
	node, err := raft.NewRaftNode(cfg, kvStore)
	if err != nil {
		log.Fatalf("failed to create raft node: %v", err)
	}

	// Start gRPC server.
	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	rpcServer := rpc.NewServer(node)
	pb.RegisterRaftKVServer(grpcServer, rpcServer)
	pb.RegisterRaftConsensusServer(grpcServer, rpcServer)

	// Start Raft in background.
	ctx, cancel := context.WithCancel(context.Background())
	go node.Run(ctx)

	// Start gRPC server in background.
	go func() {
		log.Printf("gRPC server listening on %s", cfg.ListenAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC serve failed: %v", err)
		}
	}()

	// Wait for signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("Received %v, shutting down...", sig)

	cancel()
	grpcServer.GracefulStop()
	log.Println("Server stopped.")
}

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
