#!/usr/bin/env python3
# %%

from concurrent.futures import ThreadPoolExecutor, as_completed
import tyro
from enum import Enum
from itertools import product
import json
import os
import shlex
import subprocess
import time
from dataclasses import asdict, dataclass
from dataclasses_json import dataclass_json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence, Tuple
import pandas as pd

# %%

try:
    SCRIPT_DIR = Path(__file__).parent
except NameError:
    SCRIPT_DIR = Path(os.getcwd())
PROJ_DIR = SCRIPT_DIR.resolve().parent
print(f"Project dir: {PROJ_DIR}")
os.chdir(PROJ_DIR)

# %%

@dataclass
class WorkloadConfig:
    mix_get_ratio: float
    mix_put_ratio: float
    mix_seek_ratio: float
    key_dist_a: float
    delta_bench_rowset_num: int

    key_dist_b: float = 0.0
    mix_max_scan_len: int = 64
    keyrange_num: int = 12
    keyrange_dist_a: float = 5.0
    keyrange_dist_b: float = -0.3
    keyrange_dist_c: float = -100
    keyrange_dist_d: float = -3.45
    delta_bench_rowset_trigger_percent: int = 60
    delta_bench_delta_merge_count: int = 10

    @property
    def label(self) -> str:
        r, w, s = self.mix_get_ratio, self.mix_put_ratio, self.mix_seek_ratio
        kd_a, kd_b = self.key_dist_a, self.key_dist_b
        parts = [
            f"R{int(r*10)}W{int(w*10)}S{int(s*10)}",
            f"KDA{int(kd_a)}",
            f"KDB{int(kd_b)}",
            f"rs{self.delta_bench_rowset_num}",
        ]
        return "_".join(parts)


@dataclass
class DeltaConfig:
    delta_max_partitions: int
    delta_partition_file_num_compaction_trigger: int

    delta_partition_split_growth_threshold: float = 1.5
    delta_partition_merge_growth_threshold: float = 0.5
    # enable_range_delete_compaction: bool = True
    # enable_dynamic_partition: bool = True

    @property
    def label(self) -> str:
        parts = [
            f"pt{self.delta_max_partitions}",
            f"trig{self.delta_partition_file_num_compaction_trigger}",
        ]
        if not self.delta_enable_partition_split:
            parts.append("nosplit")
        if not self.delta_enable_partition_merge:
            parts.append("nomerge")
        if not self.delta_enable_partition_compaction:
            parts.append("nopart")
        if not self.delta_enable_range_delete_compaction:
            parts.append("norangegc")
        return "_".join(parts)

class COMPACTION_STYLES(Enum):
    delta = 4
    leveled = 0

@dataclass_json
@dataclass
class BenchConfig:
    workload: WorkloadConfig
    delta: DeltaConfig

    compaction_style: COMPACTION_STYLES
    num: int
    threads: int

    benchmarks: str = "deltabench,levelstats,stats"
    key_size: int = 16
    value_size: int = 100
    bloom_bits: int = 10
    compression_type: str = "none"
    write_buffer_size: int = 32 * 1024 * 1024
    target_file_size_base: int = 32 * 1024 * 1024
    max_bytes_for_level_base: int = 128 * 1024 * 1024
    max_background_jobs: int = 8

    @property
    def label(self) -> str:
        return f"{self.compaction_style.name}_n{self.num//1000000}M_t{self.threads}_{self.workload.label}_{self.delta.label}"

    def dict(self):
        d = json.loads(self.to_json())
        workload = asdict(self.workload)
        delta = asdict(self.delta)
        del d["workload"]
        del d["delta"]
        d = {**d, **workload, **delta}
        return d

class ConfigSpace:
    compaction_style = [COMPACTION_STYLES.delta, COMPACTION_STYLES.leveled]
    num = [1_000_000, 10_000_000, 100_000_000]
    thread = [8]

    rws_ratio = [
        (0.05, 0.50, 0.45),
        (0.45, 0.50, 0.05),
        (0.25, 0.50, 0.25),
    ]
    key_dist_a = [1.0, 0.0]
    delta_bench_rowset_num = [2, 4, 8, 16]

    delta_max_partitions = [8, 16, 32, 64] # max_partitions/2 is partition num in most cases
    delta_partition_file_num_compaction_trigger = [2, 3, 4]

class DefaultConfig(ConfigSpace):
    compaction_style = [COMPACTION_STYLES.delta]
    num = [100_000_000]
    threads = [8]

    rws_ratio = [
        (0.25, 0.50, 0.25),
    ]

    key_dist_a = [1.0]
    delta_bench_rowset_num = [8]

    delta_max_partitions = [16]
    delta_partition_file_num_compaction_trigger = [4]

# 对比 Delta和Leveled 在:
class DeltaLeveledMainConfig(DefaultConfig):
    compaction_style = [COMPACTION_STYLES.delta, COMPACTION_STYLES.leveled]
    num = [1_000_000, 10_000_000, 100_000_000]

    rws_ratio = [
        (0.10, 0.50, 0.40),
        (0.40, 0.50, 0.10),
        (0.33, 0.34, 0.33),
    ]

# Delta对比均匀/热点
class DeltaKeyDistributionConfig(DefaultConfig):
    # num = [10_000_000]

    key_dist_a = [1.0, 0.0]

# Delta对比delta_partition_file_num_compaction_trigger
class DeltaCompactionTriggerConfig(DefaultConfig):
    # num = [10_000_000]

    delta_partition_file_num_compaction_trigger = [2, 3, 4]

# Delta对比delta_max_partitions
class DeltaPartitionNumConfig(DefaultConfig):
    # num = [10_000_000]

    delta_max_partitions = [8, 16, 32, 64]

EXPERIMENT_SUITES = [
    # DeltaLeveledMainConfig,
    DeltaKeyDistributionConfig,
    DeltaCompactionTriggerConfig,
    DeltaPartitionNumConfig,
]

# %%
DB_BENCH = os.environ.get("DB_BENCH", f"{PROJ_DIR / 'build/release-off/db_bench'}")
OUTPUT_DIR = Path("./benchmark_results")
BENCH_DIR = OUTPUT_DIR / "bench"
BENCH_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUTPUT_DIR / "bench.log"
CONFIGS_CSV = OUTPUT_DIR / "configs.csv"

def generate_configs_for_suite(config_space: type[ConfigSpace]) -> List[BenchConfig]:
    compaction_styles = config_space.compaction_style
    rws_ratios = config_space.rws_ratio
    key_dist_as = config_space.key_dist_a
    rowset_nums = config_space.delta_bench_rowset_num
    compaction_triggers = config_space.delta_partition_file_num_compaction_trigger
    max_partitions = config_space.delta_max_partitions
    nums = config_space.num
    threads = config_space.thread

    configs = []
    for compaction_style, key_dist_a, (get_r, put_r, seek_r), rs_num, trig, pt_num, n, t in product(
        compaction_styles, key_dist_as, rws_ratios, rowset_nums, compaction_triggers, max_partitions, nums, threads
    ):
        configs.append(BenchConfig(
            compaction_style=compaction_style,
            num=n,
            threads=t,
            workload=WorkloadConfig(
                mix_get_ratio=get_r,
                mix_put_ratio=put_r,
                mix_seek_ratio=seek_r,
                key_dist_a=key_dist_a,
                delta_bench_rowset_num=rs_num,
            ),
            delta=DeltaConfig(
                delta_partition_file_num_compaction_trigger=trig,
                delta_max_partitions=pt_num,
            ),
        ))
    return configs

def generate_configs(config_spaces = EXPERIMENT_SUITES) -> List[BenchConfig]:
    selected_spaces = config_spaces
    configs: List[BenchConfig] = []
    for config_space in selected_spaces:
        configs.extend(generate_configs_for_suite(config_space))
    return configs


def summarize_suite_sizes(config_spaces: Sequence[type[ConfigSpace]]) -> Dict[str, int]:
    return {
        config_space.__name__: len(generate_configs_for_suite(config_space))
        for config_space in config_spaces
    }

# %%

import logging

logger = logging.getLogger(Path(__file__).stem)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.DEBUG)
# Add console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
# Add file handler
fh = logging.FileHandler(LOG_FILE, mode='w')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)

# %%

def build_cmd(cfg: BenchConfig, db_dir: Path, report_file: Path) -> List[str]:
    flat: dict = json.loads(cfg.to_json())
    flat.update(flat.pop("workload"))
    flat.update(flat.pop("delta"))
    cmd = [ DB_BENCH ]
    for key, value in flat.items():
        flag = {}.get(key, key)
        cmd.append(f"--{flag}={int(value) if isinstance(value, bool) else value}")
    cmd += [
        "--statistics",
        "--histogram",
        "--perf_level=2",
        "--report_interval_seconds=5",
        f"--report_file={report_file}",
        f"--db={db_dir}",
    ]
    return cmd

def run_cmd(cmd: List[str], log_file: Path = None) -> subprocess.CompletedProcess:
    logger.info(f"Running cmd:\n{' '.join(shlex.quote(c) for c in cmd)}")
    with open(log_file, "w") if log_file else subprocess.DEVNULL as f:
        proc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
    if proc.returncode != 0:
        logger.error(f"Command failed with code {proc.returncode}, see log {log_file}")
    return proc

def run_one(cfg: BenchConfig) -> Dict:
    tag = f"{cfg.label}"
    suit_dir = BENCH_DIR / tag
    # suit_dir.mkdir(parents=True, exist_ok=True)

    if suit_dir.exists():
        logger.warning(f"Directory {suit_dir} already exists, skipping benchmark for tag={tag}")
        return None

    db_dir = suit_dir / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    report_file = suit_dir / "report.log"
    log_file = suit_dir / "db_bench.log"

    cmd = build_cmd(cfg, db_dir, report_file)

    logger.debug(f"Running tag={tag}, cmd=\n{' \\\n\t'.join(shlex.quote(c) for c in cmd)}")

    t0 = time.perf_counter()
    _ = run_cmd(cmd, log_file)
    elapsed = time.perf_counter() - t0

    flat = asdict(cfg)
    flat.update(flat.pop("workload"))
    flat.update(flat.pop("delta"))
    result = {
        "tag": tag,
        "elapsed_sec": elapsed,
        **flat,
    }
    return result

def run_sweep(configs: List[BenchConfig], dry_run: bool = False) -> List[Dict]:
    all_tasks = configs
    print(f"\n{len(all_tasks)} tasks\n")
    results = []

    if dry_run:
        return [{}]

    for cfg in all_tasks:
        r = run_one(cfg)
        if r:
            results.append(r)

    return results

# %%
def main(dry_run: bool = False):
    logger.info(f"Starting benchmark sweep with DB_BENCH={DB_BENCH}")
    suite_sizes = summarize_suite_sizes(EXPERIMENT_SUITES)
    logger.info(f"Selected suites: {suite_sizes}")
    configs = generate_configs(EXPERIMENT_SUITES)
    config_df = pd.DataFrame([{
        "tag": c.label,
        **c.dict(),
    } for c in configs])
    config_df.to_csv(CONFIGS_CSV, index=False)
    logger.debug(f"configs: \n{config_df.to_string(max_colwidth=100000)}")
    results = run_sweep(configs, dry_run=dry_run)

if __name__ == "__main__":
    tyro.cli(main)
