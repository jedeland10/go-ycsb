package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/magiconair/properties"
	"google.golang.org/grpc"

	"github.com/pingcap/go-ycsb/db/raft/raftapi"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// tweak this to suit your environment
const (
	initialWindowSize = 32 << 20 // 32 MiB per-stream window and per-connection window
)

// raftThreadKey is the context key for thread-local data
type ctxKey string

const (
	raftThreadKey ctxKey = "raftThread"
)

// threadStream holds each thread's connection, stream, and cleanup primitives
type threadStream struct {
	conn      *grpc.ClientConn
	stream    raftapi.RaftKVService_StreamProposalsClient
	cancel    context.CancelFunc
	wg        *sync.WaitGroup
	recvCount *int64
}

// raftCreator implements the ycsb.DBCreator interface
type raftCreator struct{}

type raftDB struct {
	p *properties.Properties
}

func init() {
	ycsb.RegisterDBCreator("raft", raftCreator{})
}

// NewCreator exports a creator
func NewCreator() raftCreator {
	return raftCreator{}
}

// Create stores properties; per-thread streams and connections set up in InitThread
func (c raftCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	return &raftDB{p: p}, nil
}

// InitThread opens a dedicated gRPC connection and StreamProposals for this thread
func (db *raftDB) InitThread(ctx context.Context, threadID, threadCount int) context.Context {
	// cancellable context for this stream
	tctx, cancel := context.WithCancel(ctx)

	// dial per-thread
	address := db.p.GetString("raft.address", "localhost:12380")
	conn, err := grpc.Dial(
		address,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithInitialWindowSize(initialWindowSize),
		grpc.WithInitialConnWindowSize(initialWindowSize),
	)
	if err != nil {
		log.Fatalf("thread %d: grpc.Dial error: %v", threadID, err)
	}

	// create client on this conn
	client := raftapi.NewRaftKVServiceClient(conn)

	// open bidi-stream
	stream, err := client.StreamProposals(tctx)
	if err != nil {
		conn.Close()
		log.Fatalf("thread %d: StreamProposals error: %v", threadID, err)
	}

	// drain and count acks
	var wg sync.WaitGroup
	wg.Add(1)
	var recvCount int64
	go func() {
		defer wg.Done()
		for {
			if _, err := stream.Recv(); err != nil {
				log.Printf("thread %d: Recv finished at %d acks (err=%v)", threadID, atomic.LoadInt64(&recvCount), err)
				return
			}
			atomic.AddInt64(&recvCount, 1)
		}
	}()

	// stash in context
	ts := &threadStream{conn: conn, stream: stream, cancel: cancel, wg: &wg, recvCount: &recvCount}
	return context.WithValue(ctx, raftThreadKey, ts)
}

// CleanupThread tears down this thread's stream and connection
func (db *raftDB) CleanupThread(ctx context.Context) {
	ts := ctx.Value(raftThreadKey).(*threadStream)
	ts.stream.CloseSend()
	ts.cancel()
	ts.wg.Wait()
	ts.conn.Close()
}

// Close implements ycsb.DB, nothing to do here
func (db *raftDB) Close() error {
	return nil
}

// helper to compose row keys
func getRowKey(table, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

// Update streams a PutRequest without waiting
func (db *raftDB) Update(ctx context.Context, table, key string, values map[string][]byte) error {
	rkey := getRowKey(table, key)
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	req := &raftapi.PutRequest{Key: proto.String(rkey), Value: proto.String(string(data))}

	ts := ctx.Value(raftThreadKey).(*threadStream)
	if err := ts.stream.Send(req); err != nil {
		log.Printf("thread send error: %v", err)
		return err
	}
	return nil
}

// Insert is alias for Update
func (db *raftDB) Insert(ctx context.Context, table, key string, values map[string][]byte) error {
	return db.Update(ctx, table, key, values)
}

// Delete streams an empty JSON object
func (db *raftDB) Delete(ctx context.Context, table, key string) error {
	rkey := getRowKey(table, key)
	req := &raftapi.PutRequest{Key: proto.String(rkey), Value: proto.String("{}")}
	ts := ctx.Value(raftThreadKey).(*threadStream)
	return ts.stream.Send(req)
}

// Read uses a per-thread client to perform a unary Get
func (db *raftDB) Read(ctx context.Context, table, key string, fields []string) (map[string][]byte, error) {
	rkey := getRowKey(table, key)
	ts := ctx.Value(raftThreadKey).(*threadStream)
	client := raftapi.NewRaftKVServiceClient(ts.conn)
	req := &raftapi.GetRequest{Key: proto.String(key)}
	resp, err := client.Get(ctx, req)
	if err != nil {
		return nil, err
	}
	if !resp.GetFound() {
		return nil, fmt.Errorf("could not find value for key [%s]", rkey)
	}
	var m map[string][]byte
	err = json.Unmarshal([]byte(resp.GetValue()), &m)
	return m, err
}

// Scan not implemented
func (db *raftDB) Scan(ctx context.Context, table, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan not implemented")
}
