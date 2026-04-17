#!/usr/bin/env bash
# Download a small sample from a Twitter cache trace and analyze value size distribution
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <cluster_number>"
  echo "  e.g., $0 12"
  exit 1
fi

CLUSTER=$1
URL="https://ftp.pdl.cmu.edu/pub/datasets/twemcacheWorkload/open_source/cluster${CLUSTER}.sort.zst"
SAMPLE="cluster${CLUSTER}.sample.zst"

echo ">>> Downloading first ~10MB of cluster${CLUSTER}..."
curl -s -r 0-10485760 -o "${SAMPLE}" "${URL}"

echo ">>> Analyzing value size distribution (first 200k records)..."
zstd -dc "${SAMPLE}" 2>/dev/null | head -200000 | awk -F',' '$4 > 0 {print $4}' | python3 -c "
import sys, numpy as np
sizes = [int(l.strip()) for l in sys.stdin if l.strip()]
arr = np.array(sizes)
if len(arr) == 0:
    print('No data')
    exit(1)
print(f'Non-zero value records: {len(arr):,}')
print(f'Min: {arr.min()}B, Median: {int(np.median(arr))}B, Mean: {int(np.mean(arr))}B, Max: {arr.max():,}B')
print()
print('Percentiles:')
for p in [50, 75, 90, 95, 99]:
    print(f'  p{p}: {int(np.percentile(arr, p)):,}B')
print()
buckets = [(0,1024,'<1KB'),(1024,4096,'1-4KB'),(4096,16384,'4-16KB'),
           (16384,65536,'16-64KB'),(65536,float(\"inf\"),'>64KB')]
print('Buckets:')
for lo, hi, label in buckets:
    count = np.sum((arr >= lo) & (arr < hi))
    print(f'  {label:>8}: {100*count/len(arr):5.1f}%')
"

rm -f "${SAMPLE}"
