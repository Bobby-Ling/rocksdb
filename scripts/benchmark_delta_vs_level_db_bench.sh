#!/usr/bin/env bash
# Usage: ./benchmark_delta_vs_level_hotspot.sh [output_dir]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="${1:-$ROOT_DIR/benchmark_results}"

DB_BENCH="$ROOT_DIR/build/release-off/db_bench"
# DB_BENCH="$ROOT_DIR/build/debug-asan/db_bench"
if [[ ! -x "$DB_BENCH" ]]; then
  echo "db_bench not found or not executable: $DB_BENCH"
  exit 1
fi

echo using $DB_BENCH

# =========================
# Common config
# =========================
NUM=5000000
THREADS=8
DURATION=120
KEY_SIZE=16
VALUE_SIZE=100
WRITE_BUFFER_SIZE=$((32 * 1024 * 1024))
TARGET_FILE_SIZE=$((32 * 1024 * 1024))
MAX_BYTES_FOR_LEVEL_BASE=$((128 * 1024 * 1024))
MAX_BACKGROUND_JOBS=8

# DeltaBench config
DELTA_BENCH_ROWSET_NUM=4
DELTA_BENCH_ROWSET_TRIGGER_PERCENT=60
DELTA_BENCH_DELTA_MERGE_COUNT=10

DB_BASE="$OUTPUT_DIR/tmp"
DB_DIR_DELTA="$DB_BASE/delta"
DB_DIR_LEVELED="$DB_BASE/leveled"

DELTA_LOG="$OUTPUT_DIR/delta.log"
LEVELED_LOG="$OUTPUT_DIR/leveled.log"
DELTA_REPORT="$OUTPUT_DIR/delta_report.csv"
LEVELED_REPORT="$OUTPUT_DIR/leveled_report.csv"

init_dirs() {
  rm -rf "$DB_BASE"
  mkdir -p "$DB_DIR_DELTA" "$DB_DIR_LEVELED"
  mkdir -p "$OUTPUT_DIR"
}

stats_flags=(
  "--statistics"
  "--histogram"
  "--perf_level=2"
  "--report_interval_seconds=5"
)

common_flags=(
  "--num=${NUM:-1000000}"
  "--threads=${THREADS:-1}"
  # "--duration=$DURATION"
  # "--key_size=$KEY_SIZE"
  # "--value_size=$VALUE_SIZE"
  "--write_buffer_size=${WRITE_BUFFER_SIZE:-$((64 * 1024 * 1024))}"
  "--target_file_size_base=${TARGET_FILE_SIZE:-$((64 * 1024 * 1024))}"
  "--max_bytes_for_level_base=${MAX_BYTES_FOR_LEVEL_BASE:-$((256 * 1024 * 1024))}" # for level
  "--max_background_jobs=$MAX_BACKGROUND_JOBS"
  # "--bloom_bits=10"
  "--compression_type=none"
  "${stats_flags[@]}"
)

delta_flags=(
  "--delta_max_partitions=8" # 8/2=4
  "--delta_partition_split_growth_threshold=1.5"
  "--delta_partition_merge_growth_threshold=0.5"
  "--delta_partition_target_file_size=$TARGET_FILE_SIZE"
  "--delta_partition_file_num_compaction_trigger=4"
)

delta_bench_flags=(
  "--delta_bench_rowset_num=$DELTA_BENCH_ROWSET_NUM"
  "--delta_bench_rowset_trigger_percent=$DELTA_BENCH_ROWSET_TRIGGER_PERCENT"
  "--delta_bench_delta_merge_count=$DELTA_BENCH_DELTA_MERGE_COUNT"
)

run_delta_bench() {
  echo "=== [DeltaBench] delta table benchmark ==="
  echo "  rowset_num=$DELTA_BENCH_ROWSET_NUM, trigger_percent=$DELTA_BENCH_ROWSET_TRIGGER_PERCENT%, merge_count=$DELTA_BENCH_DELTA_MERGE_COUNT"
  "$DB_BENCH" \
    --benchmarks=deltabench,levelstats,stats \
    --compaction_style=4 \
    "${common_flags[@]}" \
    "${delta_flags[@]}" \
    "${delta_bench_flags[@]}" \
    --report_file="$DELTA_REPORT" \
    --db="$DB_DIR_DELTA" \
    2>&1 | tee "$DELTA_LOG"
  cp "$DB_DIR_DELTA/LOG" "$OUTPUT_DIR/delta_db_log.log"
}

run_delta_bench_leveled() {
  echo "=== [DeltaBench Leveled] delta table benchmark (leveled compaction) ==="
  echo "  rowset_num=$DELTA_BENCH_ROWSET_NUM, trigger_percent=$DELTA_BENCH_ROWSET_TRIGGER_PERCENT%, merge_count=$DELTA_BENCH_DELTA_MERGE_COUNT"
  "$DB_BENCH" \
    --benchmarks=deltabench,levelstats,stats \
    --compaction_style=0 \
    "${common_flags[@]}" \
    "${delta_bench_flags[@]}" \
    --report_file="$LEVELED_REPORT" \
    --db="$DB_DIR_LEVELED" \
    2>&1 | tee "$LEVELED_LOG"
  cp "$DB_DIR_LEVELED/LOG" "$OUTPUT_DIR/leveled_db_log.log"
}

print_summary() {
  echo
  echo "=== Summary ==="
  echo "Delta compaction:"
  grep -E "^deltabench[[:space:]]*:" "$DELTA_LOG" | tail -1 || true
  echo
  echo "Leveled compaction:"
  grep -E "^deltabench[[:space:]]*:" "$LEVELED_LOG" | tail -1 || true
  echo
  echo "Reports:"
  echo "  $DELTA_REPORT"
  echo "  $LEVELED_REPORT"
  echo "Logs:"
  echo "  $DELTA_LOG"
  echo "  $LEVELED_LOG"
}

init_dirs
run_delta_bench
run_delta_bench_leveled
print_summary