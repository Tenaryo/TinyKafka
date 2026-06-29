#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
PRODUCER_BIN="$PROJECT_DIR/benchmark/build/producer_bench"
TINYKAFKA_BIN="$PROJECT_DIR/build/kafka"

MESSAGES=50000
SIZES=(100 1024 10240 102400)
TOPIC="bench"
TINY_PORT=9092
KAFKA_PORT=9093
KAFKA_HOME="${KAFKA_HOME:-}"

mkdir -p "$RESULTS_DIR"

cleanup() {
    echo "[compare] Cleaning up..."
    kill %1 2>/dev/null || true
    wait 2>/dev/null || true
    kill %2 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

declare -A TINY_TP
declare -A TINY_P50
declare -A KAFKA_TP
declare -A KAFKA_P50

echo "[compare] Building producer_bench..."
cmake -B "$SCRIPT_DIR/build" -S "$SCRIPT_DIR" -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build "$SCRIPT_DIR/build" --target producer_bench --target consumer_bench

# ── TinyKafka ────────────────────────────────────────────
echo "[compare] === TinyKafka (port $TINY_PORT) ==="

LOGROOT="$RESULTS_DIR/kafka-logs"
METADATA_DIR="$LOGROOT/__cluster_metadata-0"
mkdir -p "$METADATA_DIR"
python3 "$SCRIPT_DIR/setup_metadata.py" "$METADATA_DIR/00000000000000000000.log" "$TOPIC"

"$TINYKAFKA_BIN" --log.dirs="$LOGROOT" --port="$TINY_PORT" &
TINY_PID=$!
sleep 1

for SIZE in "${SIZES[@]}"; do
    TS=$(date +%Y%m%d_%H%M%S)
    CSV="$RESULTS_DIR/tiny_${SIZE}_${TS}.csv"
    echo "[compare] TinyKafka size=$SIZE"

    timeout 120 "$PRODUCER_BIN" \
        --messages="$MESSAGES" --size="$SIZE" --batch-size=1 \
        --topic="$TOPIC" --port="$TINY_PORT" --csv="$CSV" 2>/dev/null || true

    if [[ -f "$CSV" ]]; then
        TAIL=$(tail -1 "$CSV")
        IFS=',' read -r MSG SIZE_STR BATCH CONC TIME_S TP_MSG TP_MB AVG P50 P99 P999 <<<"$TAIL"
        TINY_TP[$SIZE]="$TP_MSG"
        TINY_P50[$SIZE]="$P50"
    fi
done

kill "$TINY_PID" 2>/dev/null || true
wait "$TINY_PID" 2>/dev/null || true
echo "[compare] TinyKafka done"

# ── Apache Kafka ─────────────────────────────────────────
if [[ -n "$KAFKA_HOME" && -f "$KAFKA_HOME/bin/kafka-server-start.sh" ]]; then
    echo "[compare] === Kafka (port $KAFKA_PORT) ==="

    KAFKA_DATA="/tmp/kafka-bench-data"
    rm -rf "$KAFKA_DATA"
    mkdir -p "$KAFKA_DATA"

    KAFKA_UUID=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())" | tr '[:upper:]' '[:lower:]')

    cat >"$KAFKA_DATA/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9094
listeners=PLAINTEXT://localhost:$KAFKA_PORT
controller.listener.names=PLAINTEXT
log.dirs=$KAFKA_DATA/logs
cluster.id=$KAFKA_UUID
EOF

    "$KAFKA_HOME/bin/kafka-storage.sh" format \
        --config "$KAFKA_DATA/server.properties" \
        --cluster-id "$KAFKA_UUID"

    "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_DATA/server.properties" &
    KAFKA_PID=$!
    sleep 5

    "$KAFKA_HOME/bin/kafka-topics.sh" --create \
        --topic "$TOPIC" --partitions 1 --replication-factor 1 \
        --bootstrap-server "localhost:$KAFKA_PORT"

    for SIZE in "${SIZES[@]}"; do
        TS=$(date +%Y%m%d_%H%M%S)
        CSV="$RESULTS_DIR/kafka_${SIZE}_${TS}.csv"
        echo "[compare] Kafka size=$SIZE"

        timeout 120 "$PRODUCER_BIN" \
            --messages="$MESSAGES" --size="$SIZE" --batch-size=1 \
            --topic="$TOPIC" --port="$KAFKA_PORT" --csv="$CSV" 2>/dev/null || true

        if [[ -f "$CSV" ]]; then
            TAIL=$(tail -1 "$CSV")
            IFS=',' read -r MSG SIZE_STR BATCH CONC TIME_S TP_MSG TP_MB AVG P50 P99 P999 <<<"$TAIL"
            KAFKA_TP[$SIZE]="$TP_MSG"
            KAFKA_P50[$SIZE]="$P50"
        fi
    done

    kill "$KAFKA_PID" 2>/dev/null || true
    wait "$KAFKA_PID" 2>/dev/null || true
    echo "[compare] Kafka done"
else
    echo "[compare] Kafka not available (set KAFKA_HOME to Apache Kafka install dir)"
fi

# ── Comparison Report ────────────────────────────────────
COMPARISON="$RESULTS_DIR/kafka_comparison.md"
{
    echo "## TinyKafka vs Apache Kafka — Producer Throughput Comparison"
    echo
    echo "Messages per scenario: ${MESSAGES} (single connection, batch_size=1)"
    echo
    echo "| Size  | TinyKafka msg/s | Kafka msg/s | TinyKafka P50 | Kafka P50 |"
    echo "|-------|-----------------|-------------|---------------|-----------|"

    for SIZE in "${SIZES[@]}"; do
        SIZE_LABEL="${SIZE}B"
        if [[ "$SIZE" -ge 102400 ]]; then SIZE_LABEL="100KB"
        elif [[ "$SIZE" -ge 10240 ]]; then SIZE_LABEL="10KB"
        elif [[ "$SIZE" -ge 1024 ]]; then SIZE_LABEL="1KB"
        fi

        TTP="${TINY_TP[$SIZE]:-N/A}"
        TP50="${TINY_P50[$SIZE]:-N/A}"
        KTP="${KAFKA_TP[$SIZE]:-N/A}"
        KP50="${KAFKA_P50[$SIZE]:-N/A}"

        printf "| %-5s | %-15s | %-11s | %-13s | %-9s |\n" \
            "$SIZE_LABEL" "$TTP" "$KTP" "$TP50" "$KP50"
    done

    echo
    echo "_Generated $(date -u +'%Y-%m-%dT%H:%M:%SZ')_"
} >"$COMPARISON"

echo "=== Comparison report ==="
cat "$COMPARISON"
