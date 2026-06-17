#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
KAFKA_BIN="$PROJECT_DIR/build/kafka"
BENCH_BIN="$PROJECT_DIR/benchmark/build/producer_bench"

mkdir -p "$RESULTS_DIR"

echo "[bench] Building producer_bench..."
cmake -B "$SCRIPT_DIR/build" -S "$SCRIPT_DIR" -G Ninja
cmake --build "$SCRIPT_DIR/build" --target producer_bench

echo "[bench] Starting TinyKafka server..."
"$KAFKA_BIN" --log.dirs="$LOGROOT" &
KAFKA_PID=$!

cleanup() {
    echo "[bench] Stopping server..."
    kill "$KAFKA_PID" 2>/dev/null || true
    wait "$KAFKA_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "[bench] Waiting for server on port 9092..."
for i in $(seq 1 30); do
    if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1',9092)); s.close()" 2>/dev/null; then
        break
    fi
    sleep 0.2
done

TS=$(date +%Y%m%d_%H%M%S)
REPORT="$RESULTS_DIR/bench_${TS}.md"
CSV="$RESULTS_DIR/bench_${TS}.csv"
LOGROOT="$RESULTS_DIR/kafka-logs"
TOPIC="${4:-bench}"

echo "[bench] Setting up metadata for topic '$TOPIC'..."
METADATA_DIR="$LOGROOT/__cluster_metadata-0"
mkdir -p "$METADATA_DIR"
python3 "$SCRIPT_DIR/setup_metadata.py" "$METADATA_DIR/00000000000000000000.log" "$TOPIC"

"$BENCH_BIN" \
    --messages="${1:-10000}" \
    --size="${2:-100}" \
    --batch-size="${3:-1}" \
    --topic="$TOPIC" \
    --csv="$CSV" | tee "$REPORT"

echo "[bench] Report: $REPORT"
echo "[bench] CSV:    $CSV"
