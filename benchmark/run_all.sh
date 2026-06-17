#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

MESSAGES=50000
SIZES=(100 1024 10240)
CSV_FILES=()

mkdir -p "$RESULTS_DIR"

for SIZE in "${SIZES[@]}"; do
    echo "=== Running benchmark: size=${SIZE}B ==="
    "$SCRIPT_DIR/run.sh" "$MESSAGES" "$SIZE" 1 "bench"
    # run.sh places CSV in results/ with timestamp, find the latest
    LATEST=$(ls -t "$RESULTS_DIR"/*.csv 2>/dev/null | head -1)
    if [[ -n "$LATEST" ]]; then
        CSV_FILES+=("$LATEST")
    fi
done

COMPARISON="$RESULTS_DIR/comparison.md"
{
    echo "## Producer Throughput Comparison"
    echo
    echo "Messages per scenario: ${MESSAGES}"
    echo
    echo "| Message Size | Throughput (msg/s) | Throughput (MB/s) | P50 (μs) | P99 (μs) | P999 (μs) |"
    echo "|-------------|-------------------|-------------------|---------|---------|----------|"

    for CSV in "${CSV_FILES[@]}"; do
        TAIL=$(tail -1 "$CSV")
        IFS=',' read -r MSG SIZE_STR BATCH TIME_S TP_MSG TP_MB AVG P50 P99 P999 <<<"$TAIL"
        SIZE_LABEL="${SIZE_STR}B"
        if [[ "$SIZE_STR" -ge 10240 ]]; then
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
