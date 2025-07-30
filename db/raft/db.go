package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/magiconair/properties"
	"google.golang.org/grpc"

	// Import the generated gRPC code for your RaftKV service.

	"github.com/pingcap/go-ycsb/db/raft/raftapi"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// Property keys for our raft binding.
const (
	raftAddressKey  = "raft.address"
	raftDialTimeout = "raft.dial_timeout"
)

// raftCreator implements the ycsb.DBCreator interface.
type raftCreator struct{}

type raftDB struct {
	p      *properties.Properties
	client raftapi.RaftKVServiceClient
	conn   *grpc.ClientConn
}

func init() {
	debug.SetGCPercent(300)
	ycsb.RegisterDBCreator("raft", raftCreator{})
}

// Create sets up the gRPC connection to our raft-based key–value store.
func (c raftCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	// Read properties for connection.
	address := p.GetString(raftAddressKey, "localhost:12380")
	dialTimeoutDuration := p.GetDuration(raftDialTimeout, 2*time.Second)

	// Create a context with timeout.
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeoutDuration)
	defer cancel()

	// Establish a gRPC connection. (This example uses insecure connection.)
	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}

	client := raftapi.NewRaftKVServiceClient(conn)
	return &raftDB{
		p:      p,
		client: client,
		conn:   conn,
	}, nil
}

func (db *raftDB) Close() error {
	return db.conn.Close()
}

// InitThread and CleanupThread are no-ops for this binding.
func (db *raftDB) InitThread(ctx context.Context, threadID, threadCount int) context.Context {
	return ctx
}

func (db *raftDB) CleanupThread(ctx context.Context) {}

// getRowKey creates a composite key from table and key similar to the etcd binding.
func getRowKey(table string, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

// Read queries the raft store via gRPC Get. It expects the stored value is a JSON
// encoded map[string][]byte.
func (db *raftDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	rkey := getRowKey(table, key)
	req := &raftapi.GetRequest{Key: proto.String(key)}
	resp, err := db.client.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	// If the response indicates the key wasn't found, return an error.
	if !resp.GetFound() {
		return nil, fmt.Errorf("could not find value for key [%s]", rkey)
	}

	// Decode the JSON-encoded value into a map.
	var result map[string][]byte
	err = json.NewDecoder(bytes.NewReader([]byte(resp.GetValue()))).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Scan is not fully supported here; it could be implemented if the underlying
// store supports range queries
func (db *raftDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan operation not implemented")
}

// Update encodes the provided values as JSON and sends them via Put RPC.
func (db *raftDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	rkey := getRowKey(table, key)
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	req := &raftapi.PutRequest{
		Key:   proto.String(rkey),
		Value: proto.String(string(data)),
	}
	_, err = db.client.Put(ctx, req)
	return err
}

// Insert is implemented as an Update.
func (db *raftDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.Update(ctx, table, key, values)
}

// Delete is simulated by putting an empty JSON object.
// (Adjust this behavior if your raft service supports a dedicated delete operation.)
func (db *raftDB) Delete(ctx context.Context, table string, key string) error {
	rkey := getRowKey(table, key)
	req := &raftapi.PutRequest{
		Key:   proto.String(rkey),
		Value: proto.String("{}"),
	}
	_, err := db.client.Put(ctx, req)
	return err
}

func (db *raftDB) ResetStats(ctx context.Context) error {
	_, err := db.client.ResetCacheHits(ctx, &raftapi.Empty{})
	return err
}
