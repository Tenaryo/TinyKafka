#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
KAFKA_BIN="$PROJECT_DIR/build/kafka"
BENCH_BIN="$PROJECT_DIR/benchmark/build/producer_bench"
BENCH_BIN_CONSUMER="$PROJECT_DIR/benchmark/build/consumer_bench"

LOGROOT="$RESULTS_DIR/kafka-logs"
TOPIC="bench"
MESSAGES=50000
SIZES=(100 1024 10240 102400)
PRODUCER_CSVS=()
CONSUMER_CSVS=()

mkdir -p "$RESULTS_DIR"

echo "[bench] Building..."
cmake -B "$SCRIPT_DIR/build" -S "$SCRIPT_DIR" -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build "$SCRIPT_DIR/build" --target producer_bench --target consumer_bench

for SIZE in "${SIZES[@]}"; do
    echo "=== Round: size=${SIZE}B ==="

    rm -rf "$LOGROOT"
    METADATA_DIR="$LOGROOT/__cluster_metadata-0"
    mkdir -p "$METADATA_DIR"
    python3 "$SCRIPT_DIR/setup_metadata.py" "$METADATA_DIR/00000000000000000000.log" "$TOPIC"

    "$KAFKA_BIN" --log.dirs="$LOGROOT" &
    KAFKA_PID=$!

    for i in $(seq 1 30); do
        if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1',9092)); s.close()" 2>/dev/null; then
            break
        fi
        sleep 0.2
    done

    # Producer
    TS=$(date +%Y%m%d_%H%M%S)
    CSV="$RESULTS_DIR/prod_${SIZE}_${TS}.csv"
    PRODUCER_CSVS+=("$CSV")
    "$BENCH_BIN" --messages="$MESSAGES" --size="$SIZE" --batch-size=1 --topic="$TOPIC" --csv="$CSV" \
        | tee "$RESULTS_DIR/prod_${SIZE}_${TS}.md"

    # Consumer (after produce)
    TS=$(date +%Y%m%d_%H%M%S)
    CONSUMER_CSV="$RESULTS_DIR/cons_${SIZE}_${TS}.csv"
    CONSUMER_CSVS+=("$CONSUMER_CSV")
    "$BENCH_BIN_CONSUMER" --messages="$MESSAGES" --topic="$TOPIC" --csv="$CONSUMER_CSV" \
        | tee "$RESULTS_DIR/cons_${SIZE}_${TS}.md"

    kill "$KAFKA_PID" 2>/dev/null || true
    wait "$KAFKA_PID" 2>/dev/null || true
done

COMPARISON="$RESULTS_DIR/comparison.md"
{
    echo "## Producer + Consumer Throughput (independent rounds)"
    echo
    echo "Messages per scenario: ${MESSAGES} (single connection, batch_size=1)"
    echo
    echo "| Size  | Prod msg/s | Prod MB/s | Prod P50 | Prod P99 | Cons msg/s |"
    echo "|-------|-----------|----------|---------|---------|-----------|"

    for i in "${!SIZES[@]}"; do
        SIZE="${SIZES[$i]}"
        PCSV="${PRODUCER_CSVS[$i]}"
        CCSV="${CONSUMER_CSVS[$i]}"

        SIZE_LABEL="${SIZE}B"
        if [[ "$SIZE" -ge 102400 ]]; then SIZE_LABEL="100KB"
        elif [[ "$SIZE" -ge 10240 ]]; then SIZE_LABEL="10KB"
        elif [[ "$SIZE" -ge 1024 ]]; then SIZE_LABEL="1KB"
        fi

        PTP="N/A"; PMB="N/A"; PP50="N/A"; PP99="N/A"
        if [[ -f "$PCSV" ]]; then
            PTAIL=$(tail -1 "$PCSV")
            IFS=',' read -r MSG SZ BATCH CONC TIME PTP PMB AVG PP50 PP99 P999 <<<"$PTAIL"
        fi

        CTP="N/A"
        if [[ -f "$CCSV" ]]; then
            CTAIL=$(tail -1 "$CCSV")
            IFS=',' read -r CTYPE CMSG CTIME CTP CAVG CP50 CP99 CP999 <<<"$CTAIL"
        fi

        printf "| %-5s | %-9s | %-8s | %-7s | %-7s | %-9s |\n" \
            "$SIZE_LABEL" "$PTP" "$PMB" "$PP50" "$PP99" "$CTP"
    done

    echo
    echo "_Generated $(date -u +'%Y-%m-%dT%H:%M:%SZ')_"
} >"$COMPARISON"

echo "=== Comparison report ==="
cat "$COMPARISON"
