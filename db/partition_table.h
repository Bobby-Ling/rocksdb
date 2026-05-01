#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <deque>
#include <list>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>
#include <utility>

#include "rocksdb/rocksdb_namespace.h"

#include "ylt/struct_pack.hpp"

namespace ROCKSDB_NAMESPACE {
using PartitionID = int64_t;
static constexpr PartitionID kInvalidPartitionID = -1;

struct PartitionStats {
  int64_t file_count = 0;
  uint64_t point_entries = 0;
  uint64_t total_entries = 0;
  uint64_t point_deletions = 0;
  uint64_t range_deletions = 0;
  uint64_t raw_key_size = 0;
  uint64_t raw_value_size = 0;
  uint64_t data_size = 0;

  PartitionStats& operator+=(const PartitionStats& other) {
    file_count += other.file_count;
    point_entries += other.point_entries;
    total_entries += other.total_entries;
    point_deletions += other.point_deletions;
    range_deletions += other.range_deletions;
    raw_key_size += other.raw_key_size;
    raw_value_size += other.raw_value_size;
    data_size += other.data_size;
    return *this;
  }

  PartitionStats& operator-=(const PartitionStats& other) {
    file_count -= other.file_count;
    point_entries -= other.point_entries;
    total_entries -= other.total_entries;
    point_deletions -= other.point_deletions;
    range_deletions -= other.range_deletions;
    raw_key_size -= other.raw_key_size;
    raw_value_size -= other.raw_value_size;
    data_size -= other.data_size;
    return *this;
  }

  uint64_t RawBytes() const { return raw_key_size + raw_value_size; }

  std::string DebugString() const {
    return "PartitionStats{file_count=" + std::to_string(file_count) +
           ", point_entries=" + std::to_string(point_entries) +
           ", total_entries=" + std::to_string(total_entries) +
           ", point_deletions=" + std::to_string(point_deletions) +
           ", range_deletions=" + std::to_string(range_deletions) +
           ", raw_key_size=" + std::to_string(raw_key_size) +
           ", raw_value_size=" + std::to_string(raw_value_size) +
           ", data_size=" + std::to_string(data_size) + "}";
  }
};

YLT_REFL(PartitionStats, file_count, point_entries, total_entries,
         point_deletions, range_deletions, raw_key_size, raw_value_size,
         data_size);

// A set of non-overlapping half-open user-key intervals [start, end).
// nullopt in start represents -INF; nullopt in end represents +INF.
// Intervals are maintained sorted and merged (no overlaps or adjacent segments).
struct KeyIntervalSet {
  using Bound = std::optional<std::string>;
  std::vector<std::pair<Bound, Bound>> intervals;

  // Add interval [add_start, add_end) and merge any overlapping/adjacent ones.
  void Add(const Bound& add_start, const Bound& add_end);

  // Returns true if the union of all intervals covers [left, right) entirely.
  bool CoversAll(const Bound& left, const Bound& right) const;

  bool Empty() const { return intervals.empty(); }
  void Clear() { intervals.clear(); }

  std::string DebugString() const;
};
YLT_REFL(KeyIntervalSet, intervals);

// [-INF, k1), [k1, k2), ..., [kn, +INF)
// 只存boundaries会使得分区信息不好处理;
struct PartitionInfo {
  PartitionStats stats;
  PartitionID partition_id = kInvalidPartitionID;
  std::optional<std::string> left_bound;
  // right_bound is derived dynamically from the next left boundary.
  std::optional<std::string> right_bound;
  // Accumulated range-tombstone coverage within this partition.
  // Persisted in Manifest. Reset after a RangeDelete compaction.
  KeyIntervalSet covered_ranges;
  // Recent per-flush data_size deltas applied to this partition.
  // Used to smooth RangeDelete-induced fluctuations in split / merge
  // selection. Capped at PartitionTable::stats_window_.
  std::deque<uint64_t> history_data_size;
  // Remaining apply-events before this partition is eligible for split /
  // merge again. Decremented once per PartitionTable::ApplyToNewTable call.
  uint32_t split_cooldown = 0;
  uint32_t merge_cooldown = 0;

  uint64_t MaxHistoryDataSize() const {
    if (history_data_size.empty()) return 0;
    uint64_t v = 0;
    for (auto x : history_data_size) v = std::max(v, x);
    return v;
  }
  uint64_t MinHistoryDataSize() const {
    if (history_data_size.empty()) return 0;
    uint64_t v = std::numeric_limits<uint64_t>::max();
    for (auto x : history_data_size) v = std::min(v, x);
    return v;
  }

  std::string DebugString() const {
    std::string out = "PartitionInfo{partition_id=";
    out.append(std::to_string(partition_id));
    out.append(", stats=" + stats.DebugString());
    out.append(", covered_ranges=" + covered_ranges.DebugString());
    out.append(", split_cooldown=" + std::to_string(split_cooldown));
    out.append(", merge_cooldown=" + std::to_string(merge_cooldown));
    out.append("}");
    return out;
  }
};

YLT_REFL(PartitionInfo, stats, partition_id, left_bound, right_bound,
         covered_ranges, history_data_size, split_cooldown, merge_cooldown);

class PartitionTableEdits;

// 需要db mutex_
// 抽象的分区表, 不直接涉及SST
class PartitionTable {
 public:
  friend class PartitionTableEdits;
  // RTTI仅在Debug下
  struct Plan {
    enum class Type : uint8_t { kSplit, kMerge, kPartition, kRangeDelete };
    explicit Plan(Type type) : type(type) {}
    virtual ~Plan() = default;
    virtual std::unordered_set<PartitionID> GetBusyPartitions() const = 0;
    virtual std::unordered_set<PartitionID> GetOutPartitions() const = 0;
    virtual std::string DebugString() const = 0;
    Type type;
  };

  // Intra-partition compaction: reduce sorted runs inside a single partition
  // using Universal compaction without changing partition boundaries.
  struct PartitionCompactionPlan : Plan {
    PartitionCompactionPlan() : Plan(Type::kPartition) {}
    ~PartitionCompactionPlan() override = default;
    std::unordered_set<PartitionID> GetBusyPartitions() const override {
      return {pid};
    }
    std::unordered_set<PartitionID> GetOutPartitions() const override {
      return {pid};
    }
    std::string DebugString() const override {
      return "PartitionCompactionPlan{pid=" + std::to_string(pid) + "}";
    }
    PartitionID pid;
  };

  struct SplitPlan : Plan {
    SplitPlan() : Plan(Type::kSplit) {}
    ~SplitPlan() override = default;
    std::unordered_set<PartitionID> GetBusyPartitions() const override {
      std::unordered_set<PartitionID> busy{pid};
      if (new_pid != kInvalidPartitionID) {
        busy.insert(new_pid);
      }
      return busy;
    }
    std::unordered_set<PartitionID> GetOutPartitions() const override {
      assert(new_pid != kInvalidPartitionID);
      return {pid, new_pid};
    }
    std::string DebugString() const override {
      return "SplitPlan{pid=" + std::to_string(pid) +
             ", new_pid=" + std::to_string(new_pid) + "}";
    }
    PartitionID pid;
    // TODO(lcr) new_pid1 new_pid2
    PartitionID new_pid = kInvalidPartitionID;
  };

  struct MergePlan : Plan {
    MergePlan() : Plan(Type::kMerge) {}
    ~MergePlan() override = default;
    std::unordered_set<PartitionID> GetBusyPartitions() const override {
      return {left_pid, right_pid};
    }
    std::unordered_set<PartitionID> GetOutPartitions() const override {
      return {right_pid};
    }
    std::string DebugString() const override {
      return "MergePlan{left_pid=" + std::to_string(left_pid) +
             ", right_pid=" + std::to_string(right_pid) + "}";
    }
    PartitionID left_pid;
    PartitionID right_pid;
  };

  // Compact all files in a partition to GC range-tombstone-covered keys.
  struct RangeDeleteCompactionPlan : Plan {
    RangeDeleteCompactionPlan() : Plan(Type::kRangeDelete) {}
    ~RangeDeleteCompactionPlan() override = default;
    std::unordered_set<PartitionID> GetBusyPartitions() const override {
      return {pid};
    }
    std::unordered_set<PartitionID> GetOutPartitions() const override {
      return {pid};
    }
    std::string DebugString() const override {
      return "RangeDeleteCompactionPlan{pid=" + std::to_string(pid) + "}";
    }
    PartitionID pid;
  };
  PartitionTable() = default;
  PartitionTable(uint32_t max_partitions, double split_grouth_threshold,
                 double merge_growth_threshold,
                 uint32_t file_num_compaction_trigger,
                 uint32_t stats_window = 4,
                 uint32_t split_cooldown = 4,
                 uint32_t merge_cooldown = 4,
                 bool enable_partition_split = true,
                 bool enable_partition_merge = true,
                 bool enable_partition_compaction = true,
                 bool enable_range_delete_compaction = true)
      : max_partitions_(max_partitions),
        split_growth_threshold_(split_grouth_threshold),
        merge_growth_threshold_(merge_growth_threshold),
        file_num_compaction_trigger_(
            file_num_compaction_trigger),
        stats_window_(stats_window),
        split_cooldown_(split_cooldown),
        merge_cooldown_(merge_cooldown),
        enable_partition_split_(enable_partition_split),
        enable_partition_merge_(enable_partition_merge),
        enable_partition_compaction_(enable_partition_compaction),
        enable_range_delete_compaction_(enable_range_delete_compaction) {}
  PartitionTable(const PartitionTable& other)
      : PartitionTable(other.max_partitions_, other.split_growth_threshold_,
                       other.merge_growth_threshold_,
                       other.file_num_compaction_trigger_,
                       other.stats_window_, other.split_cooldown_,
                       other.merge_cooldown_, other.enable_partition_split_,
                       other.enable_partition_merge_,
                       other.enable_partition_compaction_,
                       other.enable_range_delete_compaction_) {
    next_partition_id_ = other.next_partition_id_;
    partition_storage = other.partition_storage;
    // data_size_index = other.data_size_index;
    // pid_index = other.pid_index;

    RebuildIndex();
  }

  PartitionTable& operator=(const PartitionTable& other) noexcept {
    if (this == &other) {
      return *this;
    }
    next_partition_id_ = other.next_partition_id_;
    partition_storage = other.partition_storage;
    RebuildIndex();
    return *this;
  }

  // none moveable
  PartitionTable(PartitionTable&&) noexcept = delete;
  PartitionTable& operator=(PartitionTable&&) noexcept = delete;

  void SetOptions(uint32_t max_partitions, double split_grouth_threshold,
                  double merge_growth_threshold,
                  uint32_t file_num_compaction_trigger,
                  uint32_t stats_window = 4,
                  uint32_t split_cooldown = 4,
                  uint32_t merge_cooldown = 4,
                  bool enable_partition_split = true,
                  bool enable_partition_merge = true,
                  bool enable_partition_compaction = true,
                  bool enable_range_delete_compaction = true) {
    this->max_partitions_ = max_partitions;
    this->split_growth_threshold_ = split_grouth_threshold;
    this->merge_growth_threshold_ = merge_growth_threshold;
    this->file_num_compaction_trigger_ =
        file_num_compaction_trigger;
    this->stats_window_ = stats_window;
    this->split_cooldown_ = split_cooldown;
    this->merge_cooldown_ = merge_cooldown;
    this->enable_partition_split_ = enable_partition_split;
    this->enable_partition_merge_ = enable_partition_merge;
    this->enable_partition_compaction_ = enable_partition_compaction;
    this->enable_range_delete_compaction_ = enable_range_delete_compaction;
  };

 private:
  // TODO: user comparator
  struct Comparator {
    bool operator()(const std::optional<std::string>& a,
                    const std::optional<std::string>& b) const {
      if (!a.has_value() || !b.has_value()) {
        return a.has_value() < b.has_value();
      }
      return *a < *b;
    }
  };

  uint32_t max_partitions_ = std::numeric_limits<uint32_t>::max();
  double split_growth_threshold_ = std::numeric_limits<double>::max();
  double merge_growth_threshold_ = std::numeric_limits<double>::max();
  // Trigger intra-partition compaction when a partition's file_count reaches
  // this threshold. Defaults to max (disabled). Set via SetOptions() at
  // runtime from level0_file_num_compaction_trigger or dedicated option.
  uint32_t file_num_compaction_trigger_ =
      std::numeric_limits<uint32_t>::max();
  // Per-partition flush data_size history window size.
  uint32_t stats_window_ = 4;
  // Cooldown counts (in apply events) after split / merge.
  uint32_t split_cooldown_ = 4;
  uint32_t merge_cooldown_ = 4;
  bool enable_partition_split_ = true;
  bool enable_partition_merge_ = true;
  bool enable_partition_compaction_ = true;
  bool enable_range_delete_compaction_ = true;

  using PartitionTableStorage =
      std::map<std::optional<std::string>, PartitionInfo, Comparator>;
  PartitionTableStorage partition_storage;
  std::multimap<uint64_t, PartitionTableStorage::iterator> data_size_index;
  std::map<PartitionID, PartitionTableStorage::iterator> pid_index;

  PartitionID next_partition_id_ = 0;

 public:
  YLT_REFL(PartitionTable, partition_storage, next_partition_id_);
  using Storage = PartitionTableStorage;

  bool IsInitialized() const { return !partition_storage.empty(); }

  std::size_t NumPartitions() const { return partition_storage.size(); }

  // 一定存在
  PartitionInfo FindPartition(const std::string& user_key) const;

  std::vector<PartitionID> FindPartitionInRange(const std::optional<std::string>& left, const std::optional<std::string>& right) const;

  std::optional<PartitionInfo> GetPartition(PartitionID pid) const {
    auto id_it = FindByPid(pid);
    if (id_it == partition_storage.end()) {
      return std::nullopt;
    }
    return BuildPartitionInfo(id_it);
  }

  // Initialize with one full-range partition: [-Inf, +Inf).
  PartitionID InitFirstPartition();

  // 插入一个分区边界, 如果重复返回kInvalidPartitionID, 否则返回新分区ID
  // 需要先调用 InitFirstPartition。
  PartitionID AddPartition(const std::string& left_bound);

  // Returns the first partition id that is not currently used and is not in
  // excluded. Used by split picker to reserve an output pid that does not
  // collide with in-flight split plans from the same base version.
  PartitionID RequestNextPartitionID(
      const std::unordered_set<PartitionID>& excluded = {}) const;

  PartitionInfo GetLowestDataSizePartition() const;

  PartitionInfo GetHighestDataSizePartition() const;

  // Returns all partition IDs in order
  std::vector<PartitionID> GetPartitions() const;

  // Returns all partitions ordered by left boundary.
  std::vector<PartitionInfo> GetPartitionInfos() const;

  // Returns a compact, human-readable snapshot for debugging.
  std::string DebugString() const;

  std::shared_ptr<Plan> GetCompactionPlan(
      const std::unordered_set<PartitionID>& excluded = {}) const {
    auto compaction_type = NeedCompaction(excluded);
    if (compaction_type == CompactionType::kRangeDelete) {
      auto plan = GetRangeDeletePlan(excluded);
      if (plan) {
        return std::make_shared<RangeDeleteCompactionPlan>(*plan);
      }
    } else if (compaction_type == CompactionType::kPartition) {
      auto plan = GetPartitionCompactionPlan(excluded);
      if (plan) {
        return std::make_shared<PartitionCompactionPlan>(*plan);
      }
    } else if (compaction_type == CompactionType::kSplit) {
      auto split_plan = GetSplitPlan(excluded);
      if (split_plan) {
        return std::make_shared<SplitPlan>(*split_plan);
      }
    } else if (compaction_type == CompactionType::kMerge) {
      auto merge_plan = GetMergePlan(excluded);
      if (merge_plan) {
        return std::make_shared<MergePlan>(*merge_plan);
      }
    }
    return nullptr;
  }

  // Returns the first partition whose accumulated covered_ranges spans the
  // full partition key range. excluded partitions are skipped.
  std::optional<RangeDeleteCompactionPlan> GetRangeDeletePlan(
      const std::unordered_set<PartitionID>& excluded = {}) const;

  // 若存在 file_count >= file_num_compaction_trigger_ 的分区，
  // 则返回 file_count 最大的那个。excluded 中的分区会被跳过。
  std::optional<PartitionCompactionPlan> GetPartitionCompactionPlan(
      const std::unordered_set<PartitionID>& excluded = {}) const;

  // 若存在超过 split_thresh 的分区且当前总分区数 < max_parts，则返回候选。
  // excluded 中的分区会被跳过。
  std::optional<SplitPlan> GetSplitPlan(
      const std::unordered_set<PartitionID>& excluded = {}) const;

  // 若存在低于 merge_thresh 的分区，则返回相邻两个合并候选。
  // excluded 中的分区（及其相邻候选）会被跳过。
  std::optional<MergePlan> GetMergePlan(
      const std::unordered_set<PartitionID>& excluded = {}) const;

  enum class CompactionType : uint8_t {
    kNone = 0,
    kMerge = 1,
    kSplit = 2,
    kPartition = 3,
    kRangeDelete = 4
  };
  // excluded: 正在 compaction 的分区 ID 集合，这些分区在本次选择中会被跳过。
  CompactionType NeedCompaction(
      const std::unordered_set<PartitionID>& excluded = {}) const;
  double ComputeCompactionScore() const {
    return static_cast<double>(NeedCompaction());
  }

  void EncodeTo(std::string* dst) const;
  static bool DecodeFrom(const std::string_view& src,
                           std::shared_ptr<PartitionTable> table);

  std::shared_ptr<PartitionTable> ApplyToNewTable(const PartitionTableEdits &edits) const;

 private:
  void RebuildIndex() {
    data_size_index.clear();
    pid_index.clear();
    for (auto it = partition_storage.begin(); it != partition_storage.end(); ++it) {
      data_size_index.emplace(it->second.stats.data_size, it);
      pid_index.emplace(it->second.partition_id, it);
    }
  }
  // Remove partition identified by pid.
  void RemovePartition(PartitionID pid);

  // Update the stats of partition pid. Maintains index consistency.
  void UpdateStats(PartitionID pid, const PartitionStats& new_stats);
  PartitionTableStorage::iterator FindByPid(PartitionID pid);
  PartitionTableStorage::const_iterator FindByPid(PartitionID pid) const;

  PartitionInfo BuildPartitionInfo(
      PartitionTableStorage::const_iterator it) const;

  void EraseDataSizeIndex(PartitionTableStorage::iterator pm_it);

  // 执行拆分：在 new_boundary 处创建新分区，stats 由 CompactionJob 回填。
  PartitionID AddPartitionWithID(const std::string& left_bound,
                                 PartitionID partition_id);

  void Split(PartitionID pid, PartitionID new_pid,
             const std::string& new_boundary);

  // 执行合并：删除 left_pid，将其 stats 合并到 right_pid。
  void Merge(PartitionID left_pid, PartitionID right_pid);
};

class PartitionTableEdits {
 public:
  using Plan = PartitionTable::Plan;
  using SplitPlan = PartitionTable::SplitPlan;
  using MergePlan = PartitionTable::MergePlan;
  struct Edit {
    enum class Type : uint8_t {
      kSplit,
      kMerge,
      kPartitionUpdate,
      kPartitionSetStats,
      kInit,
      kCoverageUpdate,
      kCoverageReset
    };
    explicit Edit(Type type) : type(type) {}
    Type type;
  };
  struct Split : Edit {
    SplitPlan plan;
    std::string new_boundary;
    Split(SplitPlan plan, std::string new_boundary)
        : Edit(Type::kSplit), plan(plan), new_boundary(new_boundary) {}
  };
  struct Merge : Edit {
    MergePlan plan;
    Merge(MergePlan plan) : Edit(Type::kMerge), plan(plan) {}
  };
  struct PartitionUpdate : Edit {
    PartitionID pid;
    PartitionStats stats;
    PartitionUpdate(PartitionID pid, PartitionStats stats)
      : Edit(Type::kPartitionUpdate), pid(pid), stats(std::move(stats)) {}
  };
  struct PartitionSetStats : Edit {
    PartitionID pid;
    PartitionStats stats;
    PartitionSetStats(PartitionID pid, PartitionStats stats)
        : Edit(Type::kPartitionSetStats), pid(pid), stats(std::move(stats)) {}
  };
  struct InitPartitionTable : Edit {
    std::shared_ptr<PartitionTable> pt;
    InitPartitionTable(std::shared_ptr<PartitionTable> pt)
        : Edit(Type::kInit), pt(std::move(pt)) {}
  };

  // Flush时按分区收集统计信息，并在 edit_ 中记录分区表更新。
  void UpdatePartitionStats(PartitionID pid, const PartitionStats& stats) {
    edites_.push_back(std::make_shared<PartitionUpdate>(pid, stats));
  }

  void SetPartitionStats(PartitionID pid, const PartitionStats& stats) {
    edites_.push_back(std::make_shared<PartitionSetStats>(pid, stats));
  }

  void AddSplit(const SplitPlan& plan, const std::string& new_boundary) {
    edites_.push_back(std::make_shared<Split>(plan, new_boundary));
  }

  void AddMerge(const MergePlan& plan) {
    edites_.push_back(std::make_shared<Merge>(plan));
  }

  void AddInitPartitionTable(std::shared_ptr<PartitionTable> new_pt) {
    edites_.push_back(std::make_shared<InitPartitionTable>(std::move(new_pt)));
  }

  struct PartitionCoverageUpdate : Edit {
    PartitionID pid;
    // Intervals [start, end) to accumulate into covered_ranges.
    std::vector<std::pair<std::optional<std::string>, std::optional<std::string>>>
        ranges;
    PartitionCoverageUpdate(
        PartitionID p,
        std::vector<std::pair<std::optional<std::string>, std::optional<std::string>>>
            r)
        : Edit(Type::kCoverageUpdate), pid(p), ranges(std::move(r)) {}
  };

  struct PartitionCoverageReset : Edit {
    PartitionID pid;
    explicit PartitionCoverageReset(PartitionID p)
        : Edit(Type::kCoverageReset), pid(p) {}
  };

  void AddPartitionCoverageUpdate(
      PartitionID pid,
      std::vector<std::pair<std::optional<std::string>, std::optional<std::string>>>
          ranges) {
    edites_.push_back(
        std::make_shared<PartitionCoverageUpdate>(pid, std::move(ranges)));
  }

  void AddPartitionCoverageReset(PartitionID pid) {
    edites_.push_back(std::make_shared<PartitionCoverageReset>(pid));
  }

  auto GetEdits() const { return edites_; }

  // return "current" pt, which is const
  // auto GetPartitionTable() const { return pt_; }

 private:
  // const PartitionTable* pt_;
  std::list<std::shared_ptr<Edit>> edites_;
};

}  // namespace ROCKSDB_NAMESPACE
