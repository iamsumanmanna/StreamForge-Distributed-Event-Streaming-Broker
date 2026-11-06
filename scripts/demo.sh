#!/usr/bin/env bash
set -euo pipefail
ADDR="${1:-127.0.0.1:9092}"
go run ./cmd/produce -addr "$ADDR" -topic demo -partitions 3 -count 20
go run ./cmd/consume -addr "$ADDR" -topic demo -group g1 &
PID=$!
sleep 2
kill $PID 2>/dev/null || true
echo "demo done"
