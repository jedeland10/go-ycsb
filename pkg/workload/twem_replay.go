// Copyright 2024 Johan Edeland
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Package workload provides the twemcache trace replay workload.
// This workload replays traces from Twitter's cache-trace repository:
// https://github.com/twitter/cache-trace
//
// Trace format (CSV):
//   - timestamp: time when the cache receives the request (sec)
//   - anonymized key: the original key with anonymization
//   - key size: size of key in bytes
//   - value size: size of value in bytes
//   - client id: anonymized client ID
//   - operation: get/gets/set/add/replace/cas/append/prepend/delete/incr/decr
//   - TTL: time-to-live set by client (0 for non-write requests)

package workload

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// Property keys for trace workload configuration
const (
	// TraceFile is the path to the trace file (can be .zst compressed or plain CSV)
	TraceFile        = "trace.file"
	TraceFileDefault = ""

	// TraceTable is the table name to use for operations
	TraceTable        = "trace.table"
	TraceTableDefault = "usertable"

	// TraceMaxRecords limits the number of records to load (0 = unlimited)
	TraceMaxRecords        = "trace.maxrecords"
	TraceMaxRecordsDefault = int64(0)

	// TraceFieldName is the field name to use for values
	TraceFieldName        = "trace.fieldname"
	TraceFieldNameDefault = "field0"

	// TraceLoopReplay determines if the trace should loop when exhausted
	TraceLoopReplay        = "trace.loop"
	TraceLoopReplayDefault = false

	// TraceReadMode determines how read operations are handled:
	//   - "read": perform normal read (default)
	//   - "skip": skip read operations entirely
	//   - "write": convert reads to write operations (useful for measuring commit latency)
	TraceReadMode        = "trace.readmode"
	TraceReadModeDefault = "read"

	// TraceWriteValueSize is the value size to use when converting reads to writes (default: 1 byte)
	TraceWriteValueSize        = "trace.writevaluesize"
	TraceWriteValueSizeDefault = int64(1)
)

// traceRecord represents a single record from the trace file
type traceRecord struct {
	timestamp float64
	key       string
	keySize   int
	valueSize int
	clientID  string
	operation string
	ttl       int
}

// traceWorkload replays a twemcache trace file as a YCSB workload
type traceWorkload struct {
	p *properties.Properties

	table          string
	fieldName      string
	loopReplay     bool
	readMode       string // "read", "skip", or "write"
	writeValueSize int    // value size when converting reads to writes

	// Trace data - loaded into memory for fast access
	records    []traceRecord
	recordIdx  int64 // atomic counter for round-robin access
	numRecords int64

	// For load phase - unique keys that need to be inserted
	uniqueKeys    []string
	keySizes      map[string]int
	valueSizes    map[string]int
	loadIdx       int64 // atomic counter for load phase
	numUniqueKeys int64
}

// traceState holds per-thread state
type traceState struct {
	// Buffer for building values
	valueBuf []byte
}

type traceContextKey string

const traceStateKey = traceContextKey("trace")

// parseTraceFile reads and parses a trace file (supports .zst compression)
func parseTraceFile(filePath string, maxRecords int64) ([]traceRecord, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open trace file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file

	// Check if file is zstd compressed
	if strings.HasSuffix(filePath, ".zst") || strings.HasSuffix(filePath, ".zstd") {
		decoder, err := zstd.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
		defer decoder.Close()
		reader = decoder
	}

	// Use buffered reader for better performance
	bufReader := bufio.NewReaderSize(reader, 64*1024)
	csvReader := csv.NewReader(bufReader)

	var records []traceRecord
	lineNum := 0

	for {
		if maxRecords > 0 && int64(len(records)) >= maxRecords {
			break
		}

		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Skip malformed lines
			lineNum++
			continue
		}

		lineNum++

		// Skip if not enough fields
		if len(record) < 7 {
			continue
		}

		// Parse fields: timestamp, key, key_size, value_size, client_id, op, ttl
		timestamp, _ := strconv.ParseFloat(record[0], 64)
		key := record[1]
		keySize, _ := strconv.Atoi(record[2])
		valueSize, _ := strconv.Atoi(record[3])
		clientID := record[4]
		op := strings.ToLower(record[5])
		ttl, _ := strconv.Atoi(record[6])

		records = append(records, traceRecord{
			timestamp: timestamp,
			key:       key,
			keySize:   keySize,
			valueSize: valueSize,
			clientID:  clientID,
			operation: op,
			ttl:       ttl,
		})
	}

	return records, nil
}

// InitThread implements the Workload InitThread interface
func (w *traceWorkload) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	state := &traceState{
		valueBuf: make([]byte, 0, 4096), // Pre-allocate buffer
	}
	return context.WithValue(ctx, traceStateKey, state)
}

// CleanupThread implements the Workload CleanupThread interface
func (w *traceWorkload) CleanupThread(ctx context.Context) {
	// Nothing to clean up
}

// Close implements the Workload Close interface
func (w *traceWorkload) Close() error {
	return nil
}

// Load implements the Workload Load interface
// This pre-loads all unique keys from the trace into the database
func (w *traceWorkload) Load(ctx context.Context, db ycsb.DB, totalCount int64) error {
	return nil
}

// DoInsert implements the Workload DoInsert interface
// Used during the load phase to insert unique keys
func (w *traceWorkload) DoInsert(ctx context.Context, db ycsb.DB) error {
	idx := atomic.AddInt64(&w.loadIdx, 1) - 1
	if idx >= w.numUniqueKeys {
		return nil
	}

	key := w.uniqueKeys[idx]
	valueSize := w.valueSizes[key]
	if valueSize <= 0 {
		valueSize = 100 // Default value size
	}

	// Create value of appropriate size
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = 'x'
	}

	values := map[string][]byte{w.fieldName: value}
	return db.Insert(ctx, w.table, key, values)
}

// DoBatchInsert implements the Workload DoBatchInsert interface
func (w *traceWorkload) DoBatchInsert(ctx context.Context, batchSize int, db ycsb.DB) error {
	batchDB, ok := db.(ycsb.BatchDB)
	if !ok {
		// Fall back to single inserts
		for i := 0; i < batchSize; i++ {
			if err := w.DoInsert(ctx, db); err != nil {
				return err
			}
		}
		return nil
	}

	var keys []string
	var values []map[string][]byte

	for i := 0; i < batchSize; i++ {
		idx := atomic.AddInt64(&w.loadIdx, 1) - 1
		if idx >= w.numUniqueKeys {
			break
		}

		key := w.uniqueKeys[idx]
		valueSize := w.valueSizes[key]
		if valueSize <= 0 {
			valueSize = 100
		}

		value := make([]byte, valueSize)
		for j := range value {
			value[j] = 'x'
		}

		keys = append(keys, key)
		values = append(values, map[string][]byte{w.fieldName: value})
	}

	if len(keys) == 0 {
		return nil
	}

	return batchDB.BatchInsert(ctx, w.table, keys, values)
}

// DoTransaction implements the Workload DoTransaction interface
// This replays the next operation from the trace
func (w *traceWorkload) DoTransaction(ctx context.Context, db ycsb.DB) error {
	// Get next record using atomic counter (round-robin across threads)
	idx := atomic.AddInt64(&w.recordIdx, 1) - 1

	if idx >= w.numRecords {
		if w.loopReplay {
			// Reset to beginning
			atomic.StoreInt64(&w.recordIdx, 1)
			idx = 0
		} else {
			return nil // No more records
		}
	}

	record := w.records[idx]

	switch record.operation {
	case "get", "gets":
		switch w.readMode {
		case "skip":
			// Skip read operations entirely
			return nil
		case "write":
			// Convert read to write operation
			value := make([]byte, w.writeValueSize)
			for i := range value {
				value[i] = 'x'
			}
			values := map[string][]byte{w.fieldName: value}
			return db.Update(ctx, w.table, record.key, values)
		default: // "read"
			_, err := db.Read(ctx, w.table, record.key, []string{w.fieldName})
			return err
		}

	case "set", "add", "replace", "cas":
		valueSize := record.valueSize
		if valueSize <= 0 {
			valueSize = 100
		}

		// Get or create value buffer
		var value []byte
		if stateVal := ctx.Value(traceStateKey); stateVal != nil {
			state := stateVal.(*traceState)
			if cap(state.valueBuf) >= valueSize {
				value = state.valueBuf[:valueSize]
			} else {
				value = make([]byte, valueSize)
				state.valueBuf = value
			}
		} else {
			value = make([]byte, valueSize)
		}

		// Fill with dummy data
		for i := range value {
			value[i] = 'x'
		}

		values := map[string][]byte{w.fieldName: value}

		if record.operation == "add" {
			return db.Insert(ctx, w.table, record.key, values)
		}
		return db.Update(ctx, w.table, record.key, values)

	case "delete":
		return db.Delete(ctx, w.table, record.key)

	case "append", "prepend":
		// Treat as update
		valueSize := record.valueSize
		if valueSize <= 0 {
			valueSize = 100
		}
		value := make([]byte, valueSize)
		for i := range value {
			value[i] = 'x'
		}
		values := map[string][]byte{w.fieldName: value}
		return db.Update(ctx, w.table, record.key, values)

	case "incr", "decr":
		// Treat as read-modify-write (read then update)
		_, err := db.Read(ctx, w.table, record.key, []string{w.fieldName})
		if err != nil {
			return err
		}
		values := map[string][]byte{w.fieldName: []byte("1")}
		return db.Update(ctx, w.table, record.key, values)

	default:
		// Unknown operation, skip
		return nil
	}
}

// DoBatchTransaction implements the Workload DoBatchTransaction interface
func (w *traceWorkload) DoBatchTransaction(ctx context.Context, batchSize int, db ycsb.DB) error {
	// For simplicity, just do individual transactions
	// Could be optimized to batch similar operations together
	for i := 0; i < batchSize; i++ {
		if err := w.DoTransaction(ctx, db); err != nil {
			return err
		}
	}
	return nil
}

// traceWorkloadCreator creates the trace replay workload
type traceWorkloadCreator struct{}

// Create implements the WorkloadCreator Create interface
func (traceWorkloadCreator) Create(p *properties.Properties) (ycsb.Workload, error) {
	traceFile := p.GetString(TraceFile, TraceFileDefault)
	if traceFile == "" {
		return nil, fmt.Errorf("trace.file property is required for trace workload")
	}

	maxRecords := p.GetInt64(TraceMaxRecords, TraceMaxRecordsDefault)

	fmt.Printf("Loading trace file: %s\n", traceFile)
	records, err := parseTraceFile(traceFile, maxRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to parse trace file: %w", err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no records found in trace file")
	}

	fmt.Printf("Loaded %d records from trace\n", len(records))

	// Extract unique keys and their sizes for the load phase
	uniqueKeysMap := make(map[string]bool)
	keySizes := make(map[string]int)
	valueSizes := make(map[string]int)

	for _, r := range records {
		if !uniqueKeysMap[r.key] {
			uniqueKeysMap[r.key] = true
			keySizes[r.key] = r.keySize
		}
		// Keep track of the largest value size for each key
		if r.valueSize > valueSizes[r.key] {
			valueSizes[r.key] = r.valueSize
		}
	}

	uniqueKeys := make([]string, 0, len(uniqueKeysMap))
	for k := range uniqueKeysMap {
		uniqueKeys = append(uniqueKeys, k)
	}

	fmt.Printf("Found %d unique keys in trace\n", len(uniqueKeys))

	// Count operations by type
	readCount := 0
	writeCount := 0
	deleteCount := 0
	otherCount := 0
	for _, r := range records {
		switch r.operation {
		case "get", "gets":
			readCount++
		case "set", "add", "replace", "cas", "append", "prepend":
			writeCount++
		case "delete":
			deleteCount++
		default:
			otherCount++
		}
	}
	fmt.Printf("Operation breakdown: reads=%d (%.1f%%), writes=%d (%.1f%%), deletes=%d, other=%d\n",
		readCount, float64(readCount)*100/float64(len(records)),
		writeCount, float64(writeCount)*100/float64(len(records)),
		deleteCount, otherCount)

	readMode := strings.ToLower(p.GetString(TraceReadMode, TraceReadModeDefault))
	if readMode != "read" && readMode != "skip" && readMode != "write" {
		return nil, fmt.Errorf("invalid trace.readmode '%s', must be 'read', 'skip', or 'write'", readMode)
	}
	fmt.Printf("Read mode: %s\n", readMode)

	w := &traceWorkload{
		p:              p,
		table:          p.GetString(TraceTable, p.GetString(prop.TableName, TraceTableDefault)),
		fieldName:      p.GetString(TraceFieldName, TraceFieldNameDefault),
		loopReplay:     p.GetBool(TraceLoopReplay, TraceLoopReplayDefault),
		readMode:       readMode,
		writeValueSize: int(p.GetInt64(TraceWriteValueSize, TraceWriteValueSizeDefault)),
		records:        records,
		numRecords:     int64(len(records)),
		uniqueKeys:     uniqueKeys,
		keySizes:       keySizes,
		valueSizes:     valueSizes,
		numUniqueKeys:  int64(len(uniqueKeys)),
	}

	return w, nil
}

func init() {
	ycsb.RegisterWorkloadCreator("trace", traceWorkloadCreator{})
	ycsb.RegisterWorkloadCreator("twemcache", traceWorkloadCreator{})
}
