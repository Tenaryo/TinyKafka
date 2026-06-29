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
CSV_FILES=()

mkdir -p "$RESULTS_DIR"

echo "[bench] Building..."
cmake -B "$SCRIPT_DIR/build" -S "$SCRIPT_DIR" -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build "$SCRIPT_DIR/build" --target producer_bench --target consumer_bench

run_round() {
    local SIZE="$1"
    local BATCH="${2:-1}"
    local CONC="${3:-1}"
    local LABEL="${4:-${SIZE}B}"

    echo "=== Round: $LABEL ==="

    rm -rf "$LOGROOT"
    mkdir -p "$LOGROOT/__cluster_metadata-0"
    python3 "$SCRIPT_DIR/setup_metadata.py" "$LOGROOT/__cluster_metadata-0/00000000000000000000.log" "$TOPIC"

    "$KAFKA_BIN" --log.dirs="$LOGROOT" &
    KAFKA_PID=$!

    for i in $(seq 1 30); do
        if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1',9092)); s.close()" 2>/dev/null; then break; fi
        sleep 0.2
    done

    local TS=$(date +%Y%m%d_%H%M%S_%N)
    local PCSV="$RESULTS_DIR/prod_${TS}.csv"
    CSV_FILES+=("$PCSV")
    "$BENCH_BIN" --messages="$MESSAGES" --size="$SIZE" --batch-size="$BATCH" --concurrency="$CONC" \
        --topic="$TOPIC" --csv="$PCSV" | tee "$RESULTS_DIR/prod_${TS}.md"

    local TS2=$(date +%Y%m%d_%H%M%S_%N)
    local CCSV="$RESULTS_DIR/cons_${TS2}.csv"
    "$BENCH_BIN_CONSUMER" --messages="$MESSAGES" --topic="$TOPIC" --csv="$CCSV" \
        | tee "$RESULTS_DIR/cons_${TS2}.md"

    kill "$KAFKA_PID" 2>/dev/null || true
    wait "$KAFKA_PID" 2>/dev/null || true
}

# ── Size matrix ────────────────────────────────────────
for SIZE in "${SIZES[@]}"; do
    run_round "$SIZE" 1 1 "${SIZE}B"
done

# ── Concurrency ─────────────────────────────────────────
for CONC in 4 8; do
    run_round 1024 1 "$CONC" "1KB×C${CONC}"
done

# ── Batch size ──────────────────────────────────────────
for BATCH in 10 100; do
    run_round 1024 "$BATCH" 1 "1KB×B${BATCH}"
done

COMPARISON="$RESULTS_DIR/comparison.md"
{
    echo "## Producer Throughput (TinyKafka Release, 50K msgs)"
    echo
    echo "| Scenario          | msg/s    | MB/s    | P50  | P99  |"
    echo "|-------------------|----------|---------|------|------|"

    for CSV in "${CSV_FILES[@]}"; do
        if [[ -f "$CSV" ]]; then
            TAIL=$(tail -1 "$CSV")
            IFS=',' read -r MSG SIZE_STR BATCH CONC TIME TP MB AVG P50 P99 P999 <<<"$TAIL"

            LABEL=""
            if [[ "$BATCH" -gt 1 ]]; then
                LABEL="1KB batch=${BATCH}"
            elif [[ "$CONC" -gt 1 ]]; then
                LABEL="1KB conc=${CONC}"
            else
                SZ="$SIZE_STR"
                if [[ "$SZ" -ge 102400 ]]; then LABEL="100KB"
                elif [[ "$SZ" -ge 10240 ]]; then LABEL="10KB"
                elif [[ "$SZ" -ge 1024 ]]; then LABEL="1KB"
                else LABEL="${SZ}B"
                fi
            fi

            printf "| %-17s | %-7s | %-7s | %-4s | %-4s |\n" "$LABEL" "$TP" "$MB" "$P50" "$P99"
        fi
    done

    echo
    echo "_Generated $(date -u +'%Y-%m-%dT%H:%M:%SZ')_"
} >"$COMPARISON"

echo "=== Comparison report ==="
cat "$COMPARISON"
