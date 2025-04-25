package putbench

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/db/raft"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/spf13/cobra"
)

var PutCmd = &cobra.Command{
	Use:   "raft-put",
	Short: "Open-loop Raft put benchmark",
	Run:   runPut,
}

var (
	endpoints string
	totalOps  int
	parallel  int
	keySize   int
	valSize   int
	keySpace  int
)

func init() {
	PutCmd.Flags().StringVar(&endpoints, "endpoints", "localhost:12380", "comma-separated Raft endpoints")
	PutCmd.Flags().IntVar(&totalOps, "total", 1000000, "total number of put requests")
	PutCmd.Flags().IntVar(&parallel, "parallel", 256, "number of concurrent in-flight workers")
	PutCmd.Flags().IntVar(&keySize, "key-size", 8, "bytes per key")
	PutCmd.Flags().IntVar(&valSize, "val-size", 8, "bytes per value")
	PutCmd.Flags().IntVar(&keySpace, "key-space-size", 1, "number of distinct keys (1=always same key)")
}

func runPut(cmd *cobra.Command, _ []string) {
	// 1) build Raft binding properties
	props := properties.NewProperties()
	props.Set("raft.address", endpoints)
	props.Set("raft.dial_timeout", "2s")

	// 2) spin up N raft clients
	clients := make([]ycsb.DB, parallel)
	for i := 0; i < parallel; i++ {
		dbi, err := raft.NewCreator().Create(props)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create client %d: %v\n", i, err)
			os.Exit(1)
		}
		clients[i] = dbi
		defer dbi.Close()
	}

	// 3) pre-generate a single payload value
	value := make([]byte, valSize)
	rand.Read(value)

	// 4) prepare work channel and workers
	type workItem struct{ key, val []byte }
	reqCh := make(chan workItem, parallel)
	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func(db ycsb.DB) {
			defer wg.Done()
			for it := range reqCh {
				go func(k string, v map[string][]byte) {
					if err := db.Update(context.Background(), table, k, v); err != nil {
						log.Printf("async update error: %v", err)
					}
				}(key, values)
			}
		}(clients[i])
	}

	// 5) progress bar
	bar := pb.New(totalOps)
	bar.SetMaxWidth(80)
	bar.Start()

	// 6) key‐generation buffer
	keyBuf := make([]byte, keySize)
	var constKey []byte
	if keySpace <= 1 {
		// always use all-zero key
		constKey = make([]byte, keySize)
	}

	// 7) fire requests as fast as possible (open loop)
	start := time.Now()
	for i := 0; i < totalOps; i++ {
		var key []byte
		if keySpace > 1 {
			// pick an ID in [0, keySpace)
			id := rand.Int63n(int64(keySpace))
			// encode it big-endian into an 8-byte slice
			var id8 [8]byte
			binary.BigEndian.PutUint64(id8[:], uint64(id))

			// place that into keyBuf:
			if keySize >= 8 {
				// pad the *front* with zeros, then 8-byte ID
				copy(keyBuf[keySize-8:], id8[:])
			} else {
				// if keySize < 8, truncate the high bytes:
				copy(keyBuf, id8[8-keySize:])
			}
			// give worker its own copy
			key = append([]byte(nil), keyBuf...)
		} else {
			key = constKey
		}

		reqCh <- workItem{key: key, val: value}
		bar.Increment()
	}
	close(reqCh)

	// 8) wait for completion
	wg.Wait()
	bar.Finish()
	elapsed := time.Since(start)

	// 9) report
	fmt.Printf(
		"Done %d ops in %v → %.2f ops/sec\n",
		totalOps, elapsed,
		float64(totalOps)/elapsed.Seconds(),
	)
}
