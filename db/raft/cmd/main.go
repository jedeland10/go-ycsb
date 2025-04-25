package main

import (
	"log"

	"github.com/pingcap/go-ycsb/db/raft/cmd/putbench"
)

func main() {
	if err := putbench.PutCmd.Execute(); err != nil {
		log.Fatalf("benchmark failed: %v", err)
	}
}
