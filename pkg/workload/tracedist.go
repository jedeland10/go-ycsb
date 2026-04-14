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

// Package workload provides the tracedist workload.
// This workload generates a key pool whose key sizes follow the value-size
// distribution of a twemcache trace, then accesses keys with configurable
// zipfian skew. The stored value per key is a fixed size (default: median
// key size from the trace).

package workload

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/generator"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	// TraceDistFile is the trace file to extract value-size distribution from
	TraceDistFile        = "tracedist.file"
	TraceDistFileDefault = ""

	// TraceDistValueSize is the fixed value size written per key (default: median key size from trace)
	TraceDistValueSize        = "tracedist.valuesize"
	TraceDistValueSizeDefault = int64(0) // 0 means auto-detect from trace

	// TraceDistZipfianConstant controls the skew of key access (higher = more skewed)
	TraceDistZipfianConstant        = "tracedist.zipfianconstant"
	TraceDistZipfianConstantDefault = float64(0.99)

	// TraceDistMaxRecords limits how many trace records to read for building the key pool
	TraceDistMaxRecords        = "tracedist.maxrecords"
	TraceDistMaxRecordsDefault = int64(0) // 0 = unlimited

	// TraceDistTable is the table name for operations
	TraceDistTable        = "tracedist.table"
	TraceDistTableDefault = "usertable"

	// TraceDistFieldName is the field name for values
	TraceDistFieldName        = "tracedist.fieldname"
	TraceDistFieldNameDefault = "field0"

	// TraceDistReadProportion is the fraction of transactions that are reads (0.0 = all writes)
	TraceDistReadProportion        = "tracedist.readproportion"
	TraceDistReadProportionDefault = float64(0.0)

	// TraceDistMaxKeySize caps the maximum key size in bytes (0 = no cap)
	TraceDistMaxKeySize        = "tracedist.maxkeysize"
	TraceDistMaxKeySizeDefault = int64(0)
)

// traceDistWorkload generates keys from a trace's value-size distribution
// and accesses them with zipfian skew.
type traceDistWorkload struct {
	p *properties.Properties

	table     string
	fieldName string

	// Pre-generated key pool: each key's length = a value_size from the trace
	keys  []string
	nKeys int64

	// Fixed value written for every key
	fixedValue []byte

	// Zipfian key chooser
	keyChooser ycsb.Generator

	// Read proportion (0.0 = all writes, 1.0 = all reads)
	readProportion float64

	// Load phase counter
	loadIdx int64
}

type traceDistContextKey string

const traceDistStateKey = traceDistContextKey("tracedist")

type traceDistState struct {
	r *rand.Rand
}

func (w *traceDistWorkload) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	state := &traceDistState{
		r: rand.New(rand.NewSource(time.Now().UnixNano() + int64(threadID))),
	}
	return context.WithValue(ctx, traceDistStateKey, state)
}

func (w *traceDistWorkload) CleanupThread(ctx context.Context) {}

func (w *traceDistWorkload) Close() error { return nil }

func (w *traceDistWorkload) Load(ctx context.Context, db ycsb.DB, totalCount int64) error {
	return nil
}

// DoInsert loads a key into the database during the load phase.
func (w *traceDistWorkload) DoInsert(ctx context.Context, db ycsb.DB) error {
	idx := atomic.AddInt64(&w.loadIdx, 1) - 1
	if idx >= w.nKeys {
		return nil
	}

	if idx < 10 {
		fmt.Printf("[TRACEDIST DEBUG] DoInsert: key_len=%d value_len=%d\n", len(w.keys[idx]), len(w.fixedValue))
	}

	values := map[string][]byte{w.fieldName: w.fixedValue}
	return db.Insert(ctx, w.table, w.keys[idx], values)
}

func (w *traceDistWorkload) DoBatchInsert(ctx context.Context, batchSize int, db ycsb.DB) error {
	batchDB, ok := db.(ycsb.BatchDB)
	if !ok {
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
		if idx >= w.nKeys {
			break
		}
		keys = append(keys, w.keys[idx])
		values = append(values, map[string][]byte{w.fieldName: w.fixedValue})
	}
	if len(keys) == 0 {
		return nil
	}
	return batchDB.BatchInsert(ctx, w.table, keys, values)
}

// DoTransaction picks a key via zipfian and either reads or writes.
func (w *traceDistWorkload) DoTransaction(ctx context.Context, db ycsb.DB) error {
	state := ctx.Value(traceDistStateKey).(*traceDistState)
	idx := w.keyChooser.Next(state.r)
	key := w.keys[idx]

	if w.readProportion > 0 && state.r.Float64() < w.readProportion {
		_, err := db.Read(ctx, w.table, key, []string{w.fieldName})
		return err
	}

	values := map[string][]byte{w.fieldName: w.fixedValue}
	return db.Update(ctx, w.table, key, values)
}

func (w *traceDistWorkload) DoBatchTransaction(ctx context.Context, batchSize int, db ycsb.DB) error {
	batchDB, ok := db.(ycsb.BatchDB)
	if !ok {
		for i := 0; i < batchSize; i++ {
			if err := w.DoTransaction(ctx, db); err != nil {
				return err
			}
		}
		return nil
	}

	state := ctx.Value(traceDistStateKey).(*traceDistState)

	// Split into reads and writes
	var readKeys []string
	var writeKeys []string
	var writeValues []map[string][]byte
	for i := 0; i < batchSize; i++ {
		idx := w.keyChooser.Next(state.r)
		key := w.keys[idx]
		if w.readProportion > 0 && state.r.Float64() < w.readProportion {
			readKeys = append(readKeys, key)
		} else {
			writeKeys = append(writeKeys, key)
			writeValues = append(writeValues, map[string][]byte{w.fieldName: w.fixedValue})
		}
	}

	if len(readKeys) > 0 {
		if _, err := batchDB.BatchRead(ctx, w.table, readKeys, []string{w.fieldName}); err != nil {
			return err
		}
	}
	if len(writeKeys) > 0 {
		return batchDB.BatchUpdate(ctx, w.table, writeKeys, writeValues)
	}
	return nil
}

// traceDistCreator creates the tracedist workload.
type traceDistCreator struct{}

func (traceDistCreator) Create(p *properties.Properties) (ycsb.Workload, error) {
	traceFile := p.GetString(TraceDistFile, TraceDistFileDefault)
	if traceFile == "" {
		return nil, fmt.Errorf("tracedist.file property is required")
	}

	maxRecords := p.GetInt64(TraceDistMaxRecords, TraceDistMaxRecordsDefault)

	fmt.Printf("Loading trace file for distribution: %s\n", traceFile)
	records, err := parseTraceFile(traceFile, maxRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to parse trace file: %w", err)
	}

	maxKeySize := p.GetInt64(TraceDistMaxKeySize, TraceDistMaxKeySizeDefault)

	// Collect non-zero value sizes (these become key lengths) and all key sizes
	var keySizesForMedian []int
	var valueSizesNonZero []int
	skipped := 0
	for _, r := range records {
		keySizesForMedian = append(keySizesForMedian, r.keySize)
		if r.valueSize > 0 {
			if maxKeySize > 0 && int64(r.valueSize) > maxKeySize {
				skipped++
				continue
			}
			valueSizesNonZero = append(valueSizesNonZero, r.valueSize)
		}
	}

	if len(valueSizesNonZero) == 0 {
		return nil, fmt.Errorf("no records with non-zero value_size found in trace")
	}

	if maxKeySize > 0 {
		fmt.Printf("Max key size cap: %d bytes (skipped %d records)\n", maxKeySize, skipped)
	}
	fmt.Printf("Found %d records with non-zero value_size (key pool size)\n", len(valueSizesNonZero))

	// Determine fixed value size
	valueSize := p.GetInt64(TraceDistValueSize, TraceDistValueSizeDefault)
	if valueSize <= 0 {
		valueSize = int64(medianInt(keySizesForMedian))
		fmt.Printf("Auto-detected value size from median key size: %d bytes\n", valueSize)
	} else {
		fmt.Printf("Using configured value size: %d bytes\n", valueSize)
	}

	// Generate deterministic key pool: one key per non-zero value_size record,
	// key length = that record's value_size.
	// Use a seeded RNG so the key pool is identical across runs.
	keyRng := rand.New(rand.NewSource(42))
	keys := make([]string, len(valueSizesNonZero))
	for i, size := range valueSizesNonZero {
		buf := make([]byte, size)
		for j := range buf {
			// Printable ASCII range [33, 126] to avoid control chars and spaces
			buf[j] = byte(33 + keyRng.Intn(94))
		}
		keys[i] = string(buf)
	}

	// Build fixed value (filled with 'v')
	fixedValue := make([]byte, valueSize)
	for i := range fixedValue {
		fixedValue[i] = 'v'
	}

	// Create zipfian key chooser
	zipfianConstant := p.GetFloat64(TraceDistZipfianConstant, TraceDistZipfianConstantDefault)
	fmt.Printf("Zipfian constant: %.4f over %d keys\n", zipfianConstant, len(keys))
	keyChooser := generator.NewScrambledZipfian(0, int64(len(keys)-1), zipfianConstant)

	table := p.GetString(TraceDistTable, p.GetString(prop.TableName, TraceDistTableDefault))
	fieldName := p.GetString(TraceDistFieldName, TraceDistFieldNameDefault)
	readProportion := p.GetFloat64(TraceDistReadProportion, TraceDistReadProportionDefault)
	fmt.Printf("Read proportion: %.2f\n", readProportion)

	w := &traceDistWorkload{
		p:              p,
		table:          table,
		fieldName:      fieldName,
		keys:           keys,
		nKeys:          int64(len(keys)),
		fixedValue:     fixedValue,
		keyChooser:     keyChooser,
		readProportion: readProportion,
	}

	return w, nil
}

func init() {
	ycsb.RegisterWorkloadCreator("tracedist", traceDistCreator{})
}
