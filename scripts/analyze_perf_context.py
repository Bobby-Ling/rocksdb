#!/usr/bin/env python3
"""
Collect and analyze db_bench PERF_CONTEXT output for benchmark suites.

The script follows scripts/plot_bench.py's metadata flow: it imports the
active EXPERIMENT_SUITES from bench.py, merges benchmark metadata from
benchmark_results/configs.csv when available, parses each db_bench.log, keeps
the raw PERF_CONTEXT line, and writes parsed metrics plus pairwise Delta vs
Leveled comparisons.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJ_DIR = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

from bench import (  # noqa: E402
    BenchConfig,
    EXPERIMENT_SUITES,
    generate_configs_for_suite,
)

BENCH_DIR = PROJ_DIR / "benchmark_results" / "bench"
CONFIGS_CSV = PROJ_DIR / "benchmark_results" / "configs.csv"
OUTPUT_DIR = PROJ_DIR / "benchmark_results" / "perf_context"

STYLE_NAMES = {0: "leveled", 4: "delta"}

PERF_CONTEXT_RE = re.compile(r"PERF_CONTEXT:\s*\n([^\n]+)")
PERF_KV_RE = re.compile(
    r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)"
    r"(?=,\s*[A-Za-z_][A-Za-z0-9_]*\s*=|$)"
)
PERF_LEVEL_RE = re.compile(r"(-?\d+(?:\.\d+)?)@level(\d+)")

BENCH_SUMMARY_RE = re.compile(
    r"deltabench\s*:\s*([\d.]+)\s*micros/op\s+(\d+)\s*ops/sec\s+"
    r"([\d.]+)\s*seconds\s+(\d+)\s*operations;\s+([\d.]+)\s*MB/s"
    r"[^\n]*Gets:(\d+)\s+Puts:(\d+)\s+Seek:(\d+)"
)
LATENCY_RE = re.compile(
    r"Microseconds per (read|write|seek):\s*\n"
    r"Count:\s*(\d+)\s+Average:\s*([\d.]+)\s+StdDev:\s*([\d.]+)\s*\n"
    r"Min:\s*([\d.]+)\s+Median:\s*([\d.]+)\s+Max:\s*([\d.]+)\s*\n"
    r"Percentiles:\s+P50:\s*([\d.]+)\s+P75:\s*([\d.]+)\s+"
    r"P99:\s*([\d.]+)\s+P99\.9:\s*([\d.]+)\s+P99\.99:\s*([\d.]+)",
    re.MULTILINE,
)

PAIR_KEY_FIELDS = [
    "num",
    "threads",
    "mix_get_ratio",
    "mix_put_ratio",
    "mix_seek_ratio",
    "key_dist_a",
    "delta_bench_rowset_num",
    "delta_max_partitions",
    "delta_partition_file_num_compaction_trigger",
]

PAIR_METRICS = [
    "ops_per_sec",
    "read_p50",
    "read_p99",
    "seek_p50",
    "seek_p99",
    "write_p50",
    "write_p99",
    "pc_get_from_memtable_us_per_get",
    "pc_get_from_output_files_us_per_get",
    "pc_get_post_process_us_per_get",
    "pc_block_read_count_per_read_op",
    "pc_block_read_bytes_per_read_op",
    "pc_block_read_us_per_read_op",
    "pc_new_table_block_iter_us_per_read_op",
    "pc_block_seek_us_per_read_op",
    "pc_user_key_comparison_count_per_read_op",
    "pc_internal_key_skipped_count_per_read_op",
    "pc_internal_delete_skipped_count_per_read_op",
    "pc_seek_child_seek_us_per_seek",
    "pc_find_next_user_entry_us_per_seek",
    "pc_bloom_sst_miss_ratio",
    "pc_block_cache_hit_ratio",
]

HIGHER_IS_BETTER = {
    "ops_per_sec",
    "throughput_mb_s",
    "pc_bloom_sst_miss_ratio",
    "pc_block_cache_hit_ratio",
}


def setup_logging(out_dir: Path, level: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "perf_context_analysis.log"

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s"
    )

    file_handler = logging.FileHandler(log_path, mode="w")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    root.addHandler(console_handler)

    logging.info("logging to %s", log_path.relative_to(PROJ_DIR))


def cfg_flat(cfg: BenchConfig) -> dict[str, Any]:
    data = asdict(cfg)
    data.update(data.pop("workload"))
    data.update(data.pop("delta"))
    data["compaction_style"] = cfg.compaction_style.value
    data["compaction_style_name"] = cfg.compaction_style.name
    data["tag"] = cfg.label
    return data


def load_suite_metadata() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for suite_cls in EXPERIMENT_SUITES:
        for cfg in generate_configs_for_suite(suite_cls):
            row = cfg_flat(cfg)
            row["suite_name"] = suite_cls.__name__
            rows.append(row)
    return pd.DataFrame(rows)


def compaction_style_name(value: Any) -> str | None:
    if pd.isna(value):
        return None
    try:
        return STYLE_NAMES.get(int(value), str(value))
    except (TypeError, ValueError):
        return str(value)


def load_config_metadata(configs_csv: Path) -> pd.DataFrame:
    suite_df = load_suite_metadata()
    if configs_csv.exists():
        configs_df = pd.read_csv(configs_csv)
        logging.info("loaded %d rows from %s", len(configs_df), configs_csv)
        if "compaction_style_name" not in configs_df.columns:
            configs_df["compaction_style_name"] = configs_df["compaction_style"].map(
                compaction_style_name
            )
        suite_cols = ["tag", "suite_name"]
        metadata = configs_df.merge(suite_df[suite_cols], on="tag", how="left")
        return metadata

    logging.warning("%s not found; using bench.py suite metadata only", configs_csv)
    return suite_df


def parse_bench_log(path: Path) -> dict[str, Any]:
    if not path.exists():
        logging.warning("missing log: %s", path)
        return {}

    text = path.read_text(errors="replace")
    result: dict[str, Any] = {}

    summary = BENCH_SUMMARY_RE.search(text)
    if summary:
        result.update(
            micros_per_op=float(summary.group(1)),
            ops_per_sec=int(summary.group(2)),
            elapsed_sec=float(summary.group(3)),
            total_ops=int(summary.group(4)),
            throughput_mb_s=float(summary.group(5)),
            gets=int(summary.group(6)),
            puts=int(summary.group(7)),
            seeks=int(summary.group(8)),
        )
    else:
        logging.warning("missing deltabench summary in %s", path)

    for match in LATENCY_RE.finditer(text):
        kind = match.group(1)
        result.update(
            {
                f"{kind}_count": int(match.group(2)),
                f"{kind}_avg": float(match.group(3)),
                f"{kind}_stddev": float(match.group(4)),
                f"{kind}_min": float(match.group(5)),
                f"{kind}_median": float(match.group(6)),
                f"{kind}_max": float(match.group(7)),
                f"{kind}_p50": float(match.group(8)),
                f"{kind}_p75": float(match.group(9)),
                f"{kind}_p99": float(match.group(10)),
                f"{kind}_p999": float(match.group(11)),
                f"{kind}_p9999": float(match.group(12)),
            }
        )

    contexts = PERF_CONTEXT_RE.findall(text)
    if not contexts:
        logging.warning("missing PERF_CONTEXT in %s", path)
        return result
    if len(contexts) > 1:
        logging.warning("found %d PERF_CONTEXT blocks in %s; using the last one", len(contexts), path)

    raw_context_line = contexts[-1].strip()
    result["raw_perf_context_line"] = raw_context_line
    result.update(parse_perf_context_line(raw_context_line))
    return result


def parse_number(value: str) -> int | float | str:
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)
    return value


def parse_perf_context_line(raw_context_line: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for key, value in PERF_KV_RE.findall(raw_context_line):
        value = value.strip()
        level_values = PERF_LEVEL_RE.findall(value)
        if level_values:
            total = 0.0
            for raw_num, raw_level in level_values:
                number = float(raw_num) if "." in raw_num else int(raw_num)
                parsed[f"pc_{key}_l{raw_level}"] = number
                total += float(number)
            parsed[f"pc_{key}_levels_total"] = int(total) if total.is_integer() else total
            continue

        parsed[f"pc_{key}"] = parse_number(value)
    return parsed


def get_numeric(row: dict[str, Any] | pd.Series, key: str) -> float:
    value = row.get(key, 0)
    if pd.isna(value):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def add_derived_metrics(row: dict[str, Any]) -> dict[str, Any]:
    read_count = get_numeric(row, "read_count") or get_numeric(row, "gets")
    write_count = get_numeric(row, "write_count") or get_numeric(row, "puts")
    seek_count = get_numeric(row, "seek_count") or get_numeric(row, "seeks")
    read_ops = read_count + seek_count

    row["read_ops"] = read_ops
    row["pc_get_from_memtable_us_per_get"] = safe_div(
        get_numeric(row, "pc_get_from_memtable_time"), read_count * 1000.0
    )
    row["pc_get_from_output_files_us_per_get"] = safe_div(
        get_numeric(row, "pc_get_from_output_files_time"), read_count * 1000.0
    )
    row["pc_get_snapshot_us_per_get"] = safe_div(
        get_numeric(row, "pc_get_snapshot_time"), read_count * 1000.0
    )
    row["pc_get_post_process_us_per_get"] = safe_div(
        get_numeric(row, "pc_get_post_process_time"), read_count * 1000.0
    )

    row["pc_block_read_count_per_read_op"] = safe_div(
        get_numeric(row, "pc_block_read_count"), read_ops
    )
    row["pc_block_read_bytes_per_read_op"] = safe_div(
        get_numeric(row, "pc_block_read_byte"), read_ops
    )
    row["pc_block_read_us_per_read_op"] = safe_div(
        get_numeric(row, "pc_block_read_time"), read_ops * 1000.0
    )
    row["pc_new_table_block_iter_us_per_read_op"] = safe_div(
        get_numeric(row, "pc_new_table_block_iter_nanos"), read_ops * 1000.0
    )
    row["pc_block_seek_us_per_read_op"] = safe_div(
        get_numeric(row, "pc_block_seek_nanos"), read_ops * 1000.0
    )
    row["pc_user_key_comparison_count_per_read_op"] = safe_div(
        get_numeric(row, "pc_user_key_comparison_count"), read_ops
    )
    row["pc_internal_key_skipped_count_per_read_op"] = safe_div(
        get_numeric(row, "pc_internal_key_skipped_count"), read_ops
    )
    row["pc_internal_delete_skipped_count_per_read_op"] = safe_div(
        get_numeric(row, "pc_internal_delete_skipped_count"), read_ops
    )
    row["pc_seek_child_seek_us_per_seek"] = safe_div(
        get_numeric(row, "pc_seek_child_seek_time"), seek_count * 1000.0
    )
    row["pc_find_next_user_entry_us_per_seek"] = safe_div(
        get_numeric(row, "pc_find_next_user_entry_time"), seek_count * 1000.0
    )

    block_cache_hit = get_numeric(row, "pc_block_cache_hit_count")
    block_read = get_numeric(row, "pc_block_read_count")
    row["pc_block_cache_hit_ratio"] = safe_div(block_cache_hit, block_cache_hit + block_read)

    bloom_hit = get_numeric(row, "pc_bloom_sst_hit_count")
    bloom_miss = get_numeric(row, "pc_bloom_sst_miss_count")
    row["pc_bloom_sst_miss_ratio"] = safe_div(bloom_miss, bloom_hit + bloom_miss)

    row["pc_block_read_gib"] = get_numeric(row, "pc_block_read_byte") / (1024.0**3)
    row["pc_block_read_time_s"] = get_numeric(row, "pc_block_read_time") / 1_000_000_000.0
    row["pc_new_table_block_iter_s"] = (
        get_numeric(row, "pc_new_table_block_iter_nanos") / 1_000_000_000.0
    )
    row["pc_block_seek_s"] = get_numeric(row, "pc_block_seek_nanos") / 1_000_000_000.0
    row["pc_write_pre_post_s"] = (
        get_numeric(row, "pc_write_pre_and_post_process_time") / 1_000_000_000.0
    )
    row["pc_write_memtable_s"] = get_numeric(row, "pc_write_memtable_time") / 1_000_000_000.0
    row["pc_write_wal_s"] = get_numeric(row, "pc_write_wal_time") / 1_000_000_000.0
    row["pc_write_thread_wait_s"] = (
        get_numeric(row, "pc_write_thread_wait_nanos") / 1_000_000_000.0
    )
    row["write_ops_for_norm"] = write_count
    return row


def load_perf_context_data(bench_dir: Path, metadata: pd.DataFrame) -> pd.DataFrame:
    meta_by_tag = metadata.drop_duplicates("tag").set_index("tag") if not metadata.empty else None
    rows: list[dict[str, Any]] = []
    for result_dir in sorted(bench_dir.iterdir()):
        if not result_dir.is_dir():
            continue
        tag = result_dir.name
        log_path = result_dir / "db_bench.log"
        metrics = parse_bench_log(log_path)
        if not metrics or "raw_perf_context_line" not in metrics:
            continue

        row: dict[str, Any] = {"tag": tag, "log_path": str(log_path.relative_to(PROJ_DIR))}
        if meta_by_tag is not None and tag in meta_by_tag.index:
            row.update(meta_by_tag.loc[tag].dropna().to_dict())
        row.update(metrics)
        rows.append(add_derived_metrics(row))

    df = pd.DataFrame(rows)
    logging.info("loaded %d PERF_CONTEXT result rows", len(df))
    return df


def make_pairwise_comparison(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    group_fields = [field for field in PAIR_KEY_FIELDS if field in df.columns]
    for key, group in df.groupby(group_fields, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        by_style = {str(row["compaction_style_name"]): row for _, row in group.iterrows()}
        if "delta" not in by_style or "leveled" not in by_style:
            continue

        pair_row = dict(zip(group_fields, key))
        delta = by_style["delta"]
        leveled = by_style["leveled"]
        pair_row["delta_tag"] = delta["tag"]
        pair_row["leveled_tag"] = leveled["tag"]

        for metric in PAIR_METRICS:
            if metric not in df.columns:
                continue
            d_value = get_numeric(delta, metric)
            l_value = get_numeric(leveled, metric)
            pair_row[f"{metric}_delta"] = d_value
            pair_row[f"{metric}_leveled"] = l_value
            if l_value:
                if metric in HIGHER_IS_BETTER:
                    advantage = (d_value / l_value - 1.0) * 100.0
                else:
                    advantage = (1.0 - d_value / l_value) * 100.0
                pair_row[f"{metric}_delta_advantage_pct"] = advantage

        rows.append(pair_row)

    result = pd.DataFrame(rows)
    logging.info("built %d Delta/Leveled comparison rows", len(result))
    return result


def write_raw_context(df: pd.DataFrame, out_dir: Path) -> None:
    raw_txt = out_dir / "perf_context_raw.log"
    raw_csv = out_dir / "perf_context_raw.csv"
    with raw_txt.open("w") as output:
        for _, row in df.sort_values("tag").iterrows():
            output.write(f"===== {row['tag']} =====\n")
            output.write("PERF_CONTEXT:\n")
            output.write(str(row["raw_perf_context_line"]))
            output.write("\n\n")
    df[["tag", "log_path", "raw_perf_context_line"]].sort_values("tag").to_csv(
        raw_csv, index=False
    )
    logging.info("wrote raw PERF_CONTEXT lines to %s and %s", raw_txt, raw_csv)


def pct(value: float) -> str:
    return f"{value:+.1f}%"


def workload_label(row: pd.Series) -> str:
    return (
        f"n{int(row['num']) // 1_000_000}M "
        f"R{int(round(float(row['mix_get_ratio']) * 10))}"
        f"W{int(round(float(row['mix_put_ratio']) * 10))}"
        f"S{int(round(float(row['mix_seek_ratio']) * 10))}"
    )


def write_analysis(df: pd.DataFrame, pairs: pd.DataFrame, out_dir: Path) -> None:
    path = out_dir / "perf_context_analysis.md"
    lines: list[str] = []
    lines.append("# PERF_CONTEXT Analysis")
    lines.append("")
    lines.append(f"Loaded {len(df)} benchmark results with PERF_CONTEXT.")
    lines.append("")
    lines.append("## Outputs")
    lines.append("")
    lines.append("- `perf_context_metrics.csv`: parsed scalar and per-level PerfContext fields")
    lines.append("- `perf_context_pairs.csv`: Delta vs Leveled paired comparison")
    lines.append("- `perf_context_raw.log`: original PERF_CONTEXT log lines")
    lines.append("- `perf_context_analysis.log`: parser logging")
    lines.append("")

    if pairs.empty:
        lines.append("No Delta/Leveled pairs were available.")
    else:
        lines.append("## Pairwise Summary")
        lines.append("")
        for _, row in pairs.sort_values(["mix_get_ratio", "num"]).iterrows():
            label = workload_label(row)
            lines.append(f"### {label}")
            lines.append("")
            for metric, display in [
                ("ops_per_sec", "QPS"),
                ("read_p50", "Get P50"),
                ("seek_p50", "Seek P50"),
                ("write_p50", "Write P50"),
                ("pc_block_read_count_per_read_op", "Block reads per read op"),
                ("pc_block_read_bytes_per_read_op", "Block bytes per read op"),
                ("pc_get_from_output_files_us_per_get", "Get output-file time per Get"),
                ("pc_new_table_block_iter_us_per_read_op", "New data block iter time per read op"),
                ("pc_block_seek_us_per_read_op", "Block seek time per read op"),
            ]:
                adv_key = f"{metric}_delta_advantage_pct"
                d_key = f"{metric}_delta"
                l_key = f"{metric}_leveled"
                if adv_key not in row or pd.isna(row[adv_key]):
                    continue
                lines.append(
                    f"- {display}: delta={row[d_key]:.3f}, "
                    f"leveled={row[l_key]:.3f}, delta advantage={pct(row[adv_key])}"
                )
            lines.append("")

    path.write_text("\n".join(lines))
    logging.info("wrote analysis markdown to %s", path)


def print_console_summary(df: pd.DataFrame, pairs: pd.DataFrame) -> None:
    summary_cols = [
        "tag",
        "ops_per_sec",
        "read_p50",
        "seek_p50",
        "write_p50",
        "pc_get_from_output_files_us_per_get",
        "pc_block_read_count_per_read_op",
        "pc_block_read_bytes_per_read_op",
        "pc_block_cache_hit_ratio",
    ]
    present = [col for col in summary_cols if col in df.columns]
    print("\n=== PERF_CONTEXT Summary ===")
    print(df[present].sort_values("ops_per_sec", ascending=False).to_string(index=False))

    if pairs.empty:
        print("\nNo Delta/Leveled pairs found.")
        return

    pair_cols = [
        "num",
        "mix_get_ratio",
        "mix_seek_ratio",
        "ops_per_sec_delta_advantage_pct",
        "read_p50_delta_advantage_pct",
        "seek_p50_delta_advantage_pct",
        "write_p50_delta_advantage_pct",
        "pc_block_read_count_per_read_op_delta_advantage_pct",
        "pc_get_from_output_files_us_per_get_delta_advantage_pct",
    ]
    present_pair_cols = [col for col in pair_cols if col in pairs.columns]
    print("\n=== Delta Advantage vs Leveled (positive is better for Delta) ===")
    print(pairs[present_pair_cols].sort_values(["mix_get_ratio", "num"]).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse and analyze db_bench PERF_CONTEXT blocks."
    )
    parser.add_argument("--bench-dir", type=Path, default=BENCH_DIR)
    parser.add_argument("--configs-csv", type=Path, default=CONFIGS_CSV)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.output_dir, args.log_level)
    logging.info("Project dir: %s", PROJ_DIR)
    logging.info("Loading benchmark results from %s", args.bench_dir)

    metadata = load_config_metadata(args.configs_csv)
    df = load_perf_context_data(args.bench_dir, metadata)
    if df.empty:
        print("No PERF_CONTEXT results found.")
        return

    pairs = make_pairwise_comparison(df)

    metrics_csv = args.output_dir / "perf_context_metrics.csv"
    pairs_csv = args.output_dir / "perf_context_pairs.csv"
    df.drop(columns=["raw_perf_context_line"], errors="ignore").sort_values("tag").to_csv(
        metrics_csv, index=False
    )
    pairs.to_csv(pairs_csv, index=False)
    write_raw_context(df, args.output_dir)
    write_analysis(df, pairs, args.output_dir)

    print_console_summary(df, pairs)
    print(f"\nAll perf context outputs saved to {args.output_dir.relative_to(PROJ_DIR)}/")


if __name__ == "__main__":
    main()