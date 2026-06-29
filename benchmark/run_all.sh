#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
KAFKA_BIN="$PROJECT_DIR/build/kafka"
BENCH_BIN="$PROJECT_DIR/benchmark/build/producer_bench"

LOGROOT="$RESULTS_DIR/kafka-logs"
TOPIC="bench"
MESSAGES=50000
SIZES=(100 1024 10240 102400)
CSV_FILES=()

mkdir -p "$RESULTS_DIR"

echo "[bench] Building producer_bench..."
cmake -B "$SCRIPT_DIR/build" -S "$SCRIPT_DIR" -G Ninja
cmake --build "$SCRIPT_DIR/build" --target producer_bench

echo "[bench] Setting up metadata for topic '$TOPIC'..."
METADATA_DIR="$LOGROOT/__cluster_metadata-0"
mkdir -p "$METADATA_DIR"
python3 "$SCRIPT_DIR/setup_metadata.py" "$METADATA_DIR/00000000000000000000.log" "$TOPIC"

echo "[bench] Starting broker once for all scenarios..."
"$KAFKA_BIN" --log.dirs="$LOGROOT" &
KAFKA_PID=$!

cleanup() {
    echo "[bench] Stopping broker..."
    kill "$KAFKA_PID" 2>/dev/null || true
    wait "$KAFKA_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "[bench] Waiting for broker on port 9092..."
for i in $(seq 1 30); do
    if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1',9092)); s.close()" 2>/dev/null; then
        break
    fi
    sleep 0.2
done

for SIZE in "${SIZES[@]}"; do
    echo "=== Running benchmark: size=${SIZE}B ==="
    TS=$(date +%Y%m%d_%H%M%S)
    CSV="$RESULTS_DIR/bench_${TS}.csv"
    CSV_FILES+=("$CSV")

    "$BENCH_BIN" \
        --messages="$MESSAGES" \
        --size="$SIZE" \
        --batch-size=1 \
        --topic="$TOPIC" \
        --csv="$CSV" | tee "$RESULTS_DIR/bench_${TS}.md"
done

COMPARISON="$RESULTS_DIR/comparison.md"
{
    echo "## Producer Throughput Comparison (single broker session)"
    echo
    echo "Messages per scenario: ${MESSAGES}"
    echo
    echo "| Message Size | Throughput (msg/s) | Throughput (MB/s) | P50 (μs) | P99 (μs) | P999 (μs) |"
    echo "|-------------|-------------------|-------------------|---------|---------|----------|"

    for CSV in "${CSV_FILES[@]}"; do
        TAIL=$(tail -1 "$CSV")
        IFS=',' read -r MSG SIZE_STR BATCH CONC TIME_S TP_MSG TP_MB AVG P50 P99 P999 <<<"$TAIL"
        SIZE_LABEL="${SIZE_STR}B"
        if [[ "$SIZE_STR" -ge 102400 ]]; then
            SIZE_LABEL="100KB"
        elif [[ "$SIZE_STR" -ge 10240 ]]; then
            SIZE_LABEL="10KB"
        elif [[ "$SIZE_STR" -ge 1024 ]]; then
            SIZE_LABEL="1KB"
        fi
        printf "| %-11s | %-17s | %-17s | %-7s | %-7s | %-8s |\n" \
            "$SIZE_LABEL" "$TP_MSG" "$TP_MB" "$P50" "$P99" "$P999"
    done

    echo
    echo "_Generated $(date -u +'%Y-%m-%dT%H:%M:%SZ')_"
} >"$COMPARISON"

echo "=== Comparison report ==="
cat "$COMPARISON"
