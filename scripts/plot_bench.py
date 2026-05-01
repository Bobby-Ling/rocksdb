#!/usr/bin/env python3
"""
rocksdb-delta benchmark visualization
从 bench.py 动态读取 EXPERIMENT_SUITES 元数据，自动感知当前配置，
为每个 suite 生成对比图，保存到 benchmark_results/plots/
"""

import sys
import re
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# ── 导入 bench.py 元数据 ────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
PROJ_DIR   = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

from bench import (  # noqa: E402
    EXPERIMENT_SUITES,
    generate_configs_for_suite,
    BenchConfig,
)

BENCH_DIR  = PROJ_DIR / "benchmark_results" / "bench"
OUTPUT_DIR = PROJ_DIR / "benchmark_results" / "plots"

# ── 解析 db_bench.log ────────────────────────────────────────────────────────

def parse_log(path: Path) -> dict:
    if not path.exists():
        return {}
    text = path.read_text(errors="replace")

    m = re.search(
        r"deltabench\s*:\s*([\d.]+)\s*micros/op\s+(\d+)\s*ops/sec\s+"
        r"([\d.]+)\s*seconds\s+(\d+)\s*operations;\s+([\d.]+)\s*MB/s"
        r"[^\n]*Gets:(\d+)\s+Puts:(\d+)\s+Seek:(\d+)",
        text,
    )
    if not m:
        return {}

    result = dict(
        micros_per_op   = float(m.group(1)),
        ops_per_sec     = int(m.group(2)),
        elapsed_sec     = float(m.group(3)),
        total_ops       = int(m.group(4)),
        throughput_mb_s = float(m.group(5)),
        gets            = int(m.group(6)),
        puts            = int(m.group(7)),
        seeks           = int(m.group(8)),
    )

    for kind in ("read", "write", "seek"):
        m2 = re.search(
            rf"Microseconds per {kind}:.*?"
            r"Percentiles:\s*P50:\s*([\d.]+)\s+P75:\s*([\d.]+)\s+"
            r"P99:\s*([\d.]+)\s+P99\.9:\s*([\d.]+)\s+P99\.99:\s*([\d.]+)",
            text, re.DOTALL,
        )
        if m2:
            result.update({
                f"{kind}_p50":   float(m2.group(1)),
                f"{kind}_p75":   float(m2.group(2)),
                f"{kind}_p99":   float(m2.group(3)),
                f"{kind}_p999":  float(m2.group(4)),
                f"{kind}_p9999": float(m2.group(5)),
            })

    return result


def parse_report(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def load_data() -> pd.DataFrame:
    rows = []
    for d in sorted(BENCH_DIR.iterdir()):
        if not d.is_dir():
            continue
        metrics = parse_log(d / "db_bench.log")
        if not metrics:
            continue
        rows.append({"tag": d.name, **metrics})
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── 从 BenchConfig 展平字段 ──────────────────────────────────────────────────

_SKIP_FIELDS = {
    "benchmarks", "key_size", "value_size", "bloom_bits",
    "compression_type", "write_buffer_size", "target_file_size_base",
    "max_bytes_for_level_base", "max_background_jobs",
    "key_dist_b", "mix_max_scan_len",
    "keyrange_num", "keyrange_dist_a", "keyrange_dist_b",
    "keyrange_dist_c", "keyrange_dist_d",
    "delta_bench_delta_merge_count",
    "delta_partition_split_growth_threshold",
    "delta_partition_merge_growth_threshold",
    "threads",
}


def cfg_flat(cfg: BenchConfig) -> dict:
    d = asdict(cfg)
    d.update(d.pop("workload"))
    d.update(d.pop("delta"))
    d["compaction_style"] = cfg.compaction_style.value
    d["compaction_style_name"] = cfg.compaction_style.name
    return d


def find_varying_fields(cfgs: list[BenchConfig]) -> list[str]:
    flats = [cfg_flat(c) for c in cfgs]
    varying = []
    for k in flats[0]:
        if k in _SKIP_FIELDS or k == "compaction_style":
            continue
        if len({str(f[k]) for f in flats}) > 1:
            varying.append(k)
    return list(dict.fromkeys(varying))  # 去重同时保持顺序


def n_label(n: int) -> str:
    if n >= 1_000_000:
        return f"{n // 1_000_000}M"
    if n >= 1_000:
        return f"{n // 1_000}K"
    return str(n)


def rws_label(get_r: float, put_r: float, seek_r: float) -> str:
    return f"R{round(get_r*100):.0f}%·W{round(put_r*100):.0f}%·S{round(seek_r*100):.0f}%"


# ── 样式 ────────────────────────────────────────────────────────────────────

plt.rcParams.update({
    "figure.dpi": 150,
    "font.size": 9,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

STYLE_COLORS = {"delta": "#1976D2", "leveled": "#E53935"}
CMAP = plt.get_cmap("tab10")

LAT_FIELDS  = ["read_p50",  "read_p75",  "read_p99",  "read_p999"]
LAT_LABELS  = ["P50", "P75", "P99", "P99.9"]
SEEK_FIELDS = ["seek_p50",  "seek_p75",  "seek_p99",  "seek_p999"]
WLAT_FIELDS = ["write_p50", "write_p75", "write_p99", "write_p999"]


def _kfmt(x, _):
    return f"{int(x/1000)}K" if x >= 1000 else str(int(x))


def _bar_label(ax, bars, fmt="{:.0f}"):
    for bar in bars:
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, h * 1.01,
                    fmt.format(h), ha="center", va="bottom",
                    fontsize=7, rotation=45)


# ── Suite 1 类型：compaction_style × num × workload ─────────────────────────

def plot_suite_delta_vs_leveled(suite_cls, cfgs: list[BenchConfig],
                                 df: pd.DataFrame, out_dir: Path, idx: int):
    available = set(df["tag"])
    name = suite_cls.__name__
    workloads = suite_cls.rws_ratio
    sizes     = suite_cls.num
    styles    = suite_cls.compaction_style
    n_wl      = len(workloads)
    width     = 0.8 / len(styles)
    x         = np.arange(len(sizes))
    style_colors = [STYLE_COLORS.get(s.name, CMAP(i)) for i, s in enumerate(styles)]

    fig, axes = plt.subplots(2, n_wl, figsize=(5 * n_wl, 9), squeeze=False)
    fig.suptitle(f"Suite {idx+1} ({name}) — Delta vs Leveled",
                 fontsize=12, fontweight="bold")

    for col, (gr, pw, sr) in enumerate(workloads):
        ax_top = axes[0, col]
        ax_bot = axes[1, col]

        for si, style in enumerate(styles):
            vals = []
            for n in sizes:
                match = next((c for c in cfgs
                              if c.compaction_style == style
                              and c.num == n
                              and abs(c.workload.mix_get_ratio - gr) < 1e-3
                              and abs(c.workload.mix_put_ratio - pw) < 1e-3), None)
                if match and match.label in available:
                    vals.append(int(df[df["tag"] == match.label].iloc[0]["ops_per_sec"]))
                else:
                    vals.append(0)
            bars = ax_top.bar(x + (si - len(styles) / 2 + 0.5) * width, vals,
                              width, label=style.name,
                              color=style_colors[si], alpha=0.85)
            _bar_label(ax_top, bars)

        ax_top.set_title(rws_label(gr, pw, sr), fontsize=9)
        ax_top.set_xticks(x)
        ax_top.set_xticklabels([n_label(n) for n in sizes])
        ax_top.set_ylabel("ops/sec")
        ax_top.yaxis.set_major_formatter(mticker.FuncFormatter(_kfmt))
        ax_top.legend(fontsize=8)

        # 延迟取最大 num
        largest_n = max(sizes)
        bx = np.arange(len(LAT_LABELS)); lat_w = 0.8 / len(styles)
        for si, style in enumerate(styles):
            match = next((c for c in cfgs
                          if c.compaction_style == style
                          and c.num == largest_n
                          and abs(c.workload.mix_get_ratio - gr) < 1e-3), None)
            if match and match.label in available:
                row = df[df["tag"] == match.label].iloc[0]
                vals = [float(row.get(f, 0)) for f in LAT_FIELDS]
                bars = ax_bot.bar(bx + (si - len(styles) / 2 + 0.5) * lat_w,
                                  vals, lat_w, label=style.name,
                                  color=style_colors[si], alpha=0.85)
                _bar_label(ax_bot, bars, "{:.1f}")

        ax_bot.set_title(f"Get Latency ({n_label(largest_n)}) — {rws_label(gr,pw,sr)}", fontsize=8)
        ax_bot.set_xticks(bx); ax_bot.set_xticklabels(LAT_LABELS)
        ax_bot.set_ylabel("μs"); ax_bot.legend(fontsize=8)

    plt.tight_layout()
    out = out_dir / f"suite{idx+1}_{name}_bars.png"
    plt.savefig(out, bbox_inches="tight"); plt.close()
    print(f"  saved: {out.relative_to(PROJ_DIR)}")

    # 折线图
    fig, axes = plt.subplots(1, n_wl, figsize=(5 * n_wl, 4), squeeze=False)
    fig.suptitle(f"Suite {idx+1} — Throughput vs Dataset Size", fontsize=12, fontweight="bold")
    for col, (gr, pw, sr) in enumerate(workloads):
        ax = axes[0, col]
        for si, style in enumerate(styles):
            xs, ys = [], []
            for n in sizes:
                match = next((c for c in cfgs
                              if c.compaction_style == style
                              and c.num == n
                              and abs(c.workload.mix_get_ratio - gr) < 1e-3), None)
                if match and match.label in available:
                    xs.append(n_label(n))
                    ys.append(int(df[df["tag"] == match.label].iloc[0]["ops_per_sec"]))
            if xs:
                ax.plot(xs, ys, marker="o" if style.name == "delta" else "s",
                        color=style_colors[si], label=style.name, linewidth=2)
        ax.set_title(rws_label(gr, pw, sr))
        ax.set_xlabel("Dataset size"); ax.set_ylabel("ops/sec")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(_kfmt))
        ax.legend(fontsize=8)
    plt.tight_layout()
    out2 = out_dir / f"suite{idx+1}_{name}_scalability.png"
    plt.savefig(out2, bbox_inches="tight"); plt.close()
    print(f"  saved: {out2.relative_to(PROJ_DIR)}")


# ── 通用单维度对比 Suite ──────────────────────────────────────────────────────

def _field_display(field: str, value: Any) -> str:
    """把字段值转成可读标签"""
    if field == "key_dist_a":
        return f"{'Hotspot' if float(value) > 0 else 'Uniform'} ({field}={value:.1f})"
    if field == "delta_partition_file_num_compaction_trigger":
        return f"trig={int(value)}"
    if field == "delta_max_partitions":
        return f"pt={int(value)}"
    if field == "compaction_style_name":
        return str(value)
    return f"{field}={value}"


def plot_suite_single_dim(suite_cls, cfgs: list[BenchConfig],
                           df: pd.DataFrame, out_dir: Path, idx: int,
                           primary_field: str):
    available = set(df["tag"])
    name = suite_cls.__name__

    # 按 primary_field 的值聚合（取该字段值 → cfg）
    seen_keys: dict[Any, BenchConfig] = {}
    for cfg in cfgs:
        flat = cfg_flat(cfg)
        # primary_field 可能是 compaction_style_name
        key = flat.get(primary_field, flat.get("compaction_style_name"))
        if key not in seen_keys:
            seen_keys[key] = cfg

    ordered_keys = sorted(seen_keys.keys(), key=lambda v: str(v))
    n = len(ordered_keys)
    colors = [CMAP(i % 10) for i in range(n)]

    items = []
    for i, key in enumerate(ordered_keys):
        cfg = seen_keys[key]
        lbl = _field_display(primary_field, key)
        if cfg.label not in available:
            print(f"    [skip] {cfg.label} (not found)")
            continue
        row = df[df["tag"] == cfg.label].iloc[0].to_dict()
        items.append((lbl, row, colors[i]))

    if not items:
        print(f"  [{name}] no available data, skip"); return

    fig, axes = plt.subplots(1, 4, figsize=(18, 4))
    fig.suptitle(f"Suite {idx+1} ({name}) — varying: {primary_field}",
                 fontsize=12, fontweight="bold")

    bx = np.arange(len(LAT_LABELS))
    n_items = len(items)
    width = max(0.15, 0.8 / n_items)

    # 吞吐
    ax_t = axes[0]
    for i, (lbl, row, color) in enumerate(items):
        bar = ax_t.bar(i, row.get("ops_per_sec", 0), color=color, alpha=0.85, label=lbl)
        _bar_label(ax_t, bar)
    ax_t.set_title("Throughput"); ax_t.set_ylabel("ops/sec")
    ax_t.yaxis.set_major_formatter(mticker.FuncFormatter(_kfmt))
    ax_t.set_xticks(range(n_items)); ax_t.set_xticklabels([it[0] for it in items], rotation=15, ha="right")
    ax_t.legend(fontsize=7)

    # Get/Seek/写延迟
    latency_panels = [
        (LAT_FIELDS, "Get Latency"),
        (SEEK_FIELDS, "Seek Latency"),
        (WLAT_FIELDS, "Write Latency"),
    ]
    for col, (lats, title) in enumerate(latency_panels, 1):
        ax = axes[col]
        for i, (lbl, row, color) in enumerate(items):
            vals = [row.get(f, 0) for f in lats]
            offset = (i - n_items / 2 + 0.5) * width
            bars = ax.bar(bx + offset, vals, width, color=color, alpha=0.85, label=lbl)
            _bar_label(ax, bars, "{:.1f}")
        ax.set_xticks(bx); ax.set_xticklabels(LAT_LABELS)
        ax.set_ylabel("μs"); ax.set_title(title); ax.legend(fontsize=7)

    plt.tight_layout()
    out = out_dir / f"suite{idx+1}_{name}.png"
    plt.savefig(out, bbox_inches="tight"); plt.close()
    print(f"  saved: {out.relative_to(PROJ_DIR)}")


# ── 时序 QPS ─────────────────────────────────────────────────────────────────

def plot_timeseries_for_suite(cfgs: list[BenchConfig], df: pd.DataFrame,
                               out_dir: Path, idx: int, name: str,
                               varying: list[str]):
    available = set(df["tag"])
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.set_title(f"Suite {idx+1} ({name}) — QPS over time",
                 fontsize=11, fontweight="bold")

    plotted = 0
    for i, cfg in enumerate(cfgs):
        if cfg.label not in available:
            continue
        rpt = parse_report(BENCH_DIR / cfg.label / "report.log")
        if rpt is None or rpt.empty:
            continue
        flat = cfg_flat(cfg)
        lbl_parts = [_field_display(f, flat.get(f, flat.get("compaction_style_name")))
                     for f in varying]
        lbl = " | ".join(lbl_parts) if lbl_parts else cfg.label
        ax.plot(rpt["secs_elapsed"] / 60, rpt["interval_qps"],
                label=lbl, alpha=0.75, linewidth=1.2, color=CMAP(i % 10))
        plotted += 1

    if plotted == 0:
        plt.close(); return

    ax.set_xlabel("Time (minutes)"); ax.set_ylabel("Interval QPS")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_kfmt))
    ax.legend(fontsize=7, ncol=2)
    plt.tight_layout()
    out = out_dir / f"suite{idx+1}_{name}_timeseries.png"
    plt.savefig(out, bbox_inches="tight"); plt.close()
    print(f"  saved: {out.relative_to(PROJ_DIR)}")


# ── Suite 分发 ────────────────────────────────────────────────────────────────

# 预定义各 suite 类的主对比字段（未知 suite 会自动推断）
_PRIMARY_FIELD_MAP = {
    "DeltaLeveledMainConfig":       None,   # special
    "DeltaKeyDistributionConfig":   "key_dist_a",
    "DeltaCompactionTriggerConfig": "delta_partition_file_num_compaction_trigger",
    "DeltaPartitionNumConfig":      "delta_max_partitions",
}


def dispatch_suite(suite_cls, cfgs: list[BenchConfig],
                   df: pd.DataFrame, out_dir: Path, idx: int):
    name = suite_cls.__name__
    varying = find_varying_fields(cfgs)
    print(f"  varying fields: {varying}")

    # 判断是否是 DeltaLeveled 类型（同时有 compaction_style 和 num 变化）
    is_delta_leveled = ("compaction_style_name" in varying and "num" in varying)

    if is_delta_leveled and hasattr(suite_cls, "rws_ratio"):
        plot_suite_delta_vs_leveled(suite_cls, cfgs, df, out_dir, idx)
    else:
        # 获取主对比字段
        primary = _PRIMARY_FIELD_MAP.get(name)
        # 自动推断：从 varying 中选第一个有意义的字段
        if primary is None or primary not in varying:
            candidates = [f for f in varying if f not in ("compaction_style_name",)]
            primary = candidates[0] if candidates else (varying[0] if varying else None)
        if primary is None:
            print(f"  [{name}] cannot determine primary field, skip"); return
        plot_suite_single_dim(suite_cls, cfgs, df, out_dir, idx, primary)

    plot_timeseries_for_suite(cfgs, df, out_dir, idx, name, varying)


# ── 总结表 ───────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame):
    cols = ["tag", "ops_per_sec", "throughput_mb_s",
        "read_p50", "read_p99", "seek_p50", "seek_p99",
        "write_p50", "write_p99"]
    present = [c for c in cols if c in df.columns]
    print("\n=== Summary (read_* = Get latency) ===")
    print(df[present].sort_values("ops_per_sec", ascending=False).to_string(index=False))


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Loading results from {BENCH_DIR} ...")
    df = load_data()
    if df.empty:
        print("No results found."); return
    print(f"Loaded {len(df)} benchmark results.")
    print_summary(df)

    print(f"\nExperiment suites: {[s.__name__ for s in EXPERIMENT_SUITES]}")

    for idx, suite_cls in enumerate(EXPERIMENT_SUITES):
        print(f"\n[Suite {idx+1}] {suite_cls.__name__}")
        cfgs = generate_configs_for_suite(suite_cls)
        available = [c for c in cfgs if c.label in set(df["tag"])]
        print(f"  configs: {len(cfgs)} total, {len(available)} available")
        if not available:
            print("  no available data, skip"); continue
        dispatch_suite(suite_cls, cfgs, df, OUTPUT_DIR, idx)

    print(f"\nAll plots saved to benchmark_results/plots/")


if __name__ == "__main__":
    main()
