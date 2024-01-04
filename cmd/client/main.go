package main

import (
	"context"
	"fmt"
	"os"
	"time"

	pb "github.com/nikhilghind/raft-kv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  raft-kv-client --server <addr> put <key> <value>
  raft-kv-client --server <addr> get <key>
  raft-kv-client --server <addr> delete <key>

Options:
  --server <addr>   Server address (default: localhost:50051)
`)
	os.Exit(1)
}

func main() {
	args := os.Args[1:]
	server := "localhost:50051"
	maxRedirects := 3

	// Parse --server flag.
	var cmdArgs []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--server" && i+1 < len(args) {
			server = args[i+1]
			i++
		} else {
			cmdArgs = append(cmdArgs, args[i])
		}
	}

	if len(cmdArgs) < 1 {
		usage()
	}

	command := cmdArgs[0]

	for attempt := 0; attempt <= maxRedirects; attempt++ {
		conn, err := grpc.NewClient(server,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to %s: %v\n", server, err)
			os.Exit(1)
		}

		client := pb.NewRaftKVClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		switch command {
		case "put":
			if len(cmdArgs) < 3 {
				cancel()
				conn.Close()
				fmt.Fprintln(os.Stderr, "Usage: put <key> <value>")
				os.Exit(1)
			}
			resp, err := client.Put(ctx, &pb.PutRequest{Key: cmdArgs[1], Value: cmdArgs[2]})
			cancel()
			conn.Close()
			if err != nil {
				fmt.Fprintf(os.Stderr, "RPC error: %v\n", err)
				os.Exit(1)
			}
			if !resp.Success && resp.LeaderHint != "" {
				fmt.Fprintf(os.Stderr, "Redirecting to leader at %s\n", resp.LeaderHint)
				server = resp.LeaderHint
				continue
			}
			if !resp.Success {
				fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
				os.Exit(1)
			}
			fmt.Println("OK")
			return

		case "get":
			if len(cmdArgs) < 2 {
				cancel()
				conn.Close()
				fmt.Fprintln(os.Stderr, "Usage: get <key>")
				os.Exit(1)
			}
			resp, err := client.Get(ctx, &pb.GetRequest{Key: cmdArgs[1]})
			cancel()
			conn.Close()
			if err != nil {
				fmt.Fprintf(os.Stderr, "RPC error: %v\n", err)
				os.Exit(1)
			}
			if resp.Error != "" {
				fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
				os.Exit(1)
			}
			if !resp.Found {
				fmt.Println("(not found)")
				return
			}
			fmt.Println(resp.Value)
			return

		case "delete":
			if len(cmdArgs) < 2 {
				cancel()
				conn.Close()
				fmt.Fprintln(os.Stderr, "Usage: delete <key>")
				os.Exit(1)
			}
			resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: cmdArgs[1]})
			cancel()
			conn.Close()
			if err != nil {
				fmt.Fprintf(os.Stderr, "RPC error: %v\n", err)
				os.Exit(1)
			}
			if !resp.Success && resp.LeaderHint != "" {
				fmt.Fprintf(os.Stderr, "Redirecting to leader at %s\n", resp.LeaderHint)
				server = resp.LeaderHint
				continue
			}
			if !resp.Success {
				fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
				os.Exit(1)
			}
			fmt.Println("OK")
			return

		default:
			fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
			usage()
		}
	}

	fmt.Fprintln(os.Stderr, "Too many redirects")
	os.Exit(1)
}
