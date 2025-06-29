// get_cache_hits.go
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/pingcap/go-ycsb/db/raft/raftapi"
	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", "localhost:5000", "raft gRPC endpoint")
	flag.Parse()

	conn, err := grpc.Dial(*addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("failed to dial %s: %v", *addr, err)
	}
	defer conn.Close()

	cli := raftapi.NewRaftKVServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.GetCacheHits(ctx, &raftapi.Empty{})
	if err != nil {
		log.Fatalf("GetCacheHits RPC failed: %v", err)
	}

	log.Printf("<<< cache hits: %d", resp.GetCachehits())
}
