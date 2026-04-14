#pragma once

#include <cassert>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

#include "ylt/struct_pack.hpp"

namespace ROCKSDB_NAMESPACE {
using PartitionID = int64_t;
static constexpr PartitionID kInvalidPartitionID = -1;
// [-INF, k1), [k1, k2), ..., [kn, +INF)
// 只存boundaries会使得分区信息不好处理;
struct PartitionInfo {
  int64_t growth_rate = 0;
  PartitionID partition_id = kInvalidPartitionID;
  std::optional<std::string> left_bound;
  // right_bound is derived dynamically from the next left boundary.
  std::optional<std::string> right_bound;
};

class PartitionTableEdits;

// 需要db mutex_
// 抽象的分区表, 不直接涉及SST
class PartitionTable {
 public:
  friend class PartitionTableEdits;
  // RTTI仅在Debug下
  struct Plan {
    enum class Type : uint8_t { kSplit, kMerge };
    explicit Plan(Type type) : type(type) {}
    virtual ~Plan() = default;
    virtual std::string DebugString() const = 0;
    Type type;
  };

  struct SplitPlan : Plan {
    SplitPlan() : Plan(Type::kSplit) {}
    ~SplitPlan() override = default;
    std::string DebugString() const override {
      return "SplitPlan{pid=" + std::to_string(pid) + "}";
    }
    PartitionID pid;
    // TODO(lcr) new_pid1 new_pid2
    PartitionID new_pid = kInvalidPartitionID;
  };

  struct MergePlan : Plan {
    MergePlan() : Plan(Type::kMerge) {}
    ~MergePlan() override = default;
    std::string DebugString() const override {
      return "MergePlan{left_pid=" + std::to_string(left_pid) +
             ", right_pid=" + std::to_string(right_pid) + "}";
    }
    PartitionID left_pid;
    PartitionID right_pid;
  };
  PartitionTable() = default;
  PartitionTable(uint32_t max_partitions,
                          double split_grouth_threshold,
                          double merge_growth_threshold)
      : max_partitions_(max_partitions),
        split_growth_threshold_(split_grouth_threshold),
        merge_growth_threshold_(merge_growth_threshold) {}
  PartitionTable(const PartitionTable& other)
      : PartitionTable(other.max_partitions_, other.split_growth_threshold_,
                       other.merge_growth_threshold_) {
    next_partition_id_ = other.next_partition_id_;
    partition_storage = other.partition_storage;
    // growth_rate_index = other.growth_rate_index;
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
                  double merge_growth_threshold) {
    this->max_partitions_ = max_partitions;
    this->split_growth_threshold_ = split_grouth_threshold;
    this->merge_growth_threshold_ = merge_growth_threshold;
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

  using PartitionTableStorage =
      std::map<std::optional<std::string>, PartitionInfo, Comparator>;
  PartitionTableStorage partition_storage;
  std::multimap<int64_t, PartitionTableStorage::iterator> growth_rate_index;
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

  PartitionInfo GetLowestGrowthPartition() const;

  PartitionInfo GetHighestGrowthPartition() const;

  // Returns all partition IDs in order
  std::vector<PartitionID> GetPartitions() const;

  // Returns all partitions ordered by left boundary.
  std::vector<PartitionInfo> GetPartitionInfos() const;

  // Returns a compact, human-readable snapshot for debugging.
  std::string DebugString() const;

  std::shared_ptr<Plan> GetCompactionPlan() const {
    auto compaction_type = NeedCompaction();
    if (compaction_type == CompactionType::kSplit) {
      auto split_plan = GetSplitPlan();
      if (split_plan) {
        return std::make_shared<SplitPlan>(*split_plan);
      }
    } else if (compaction_type == CompactionType::kMerge) {
      auto merge_plan = GetMergePlan();
      if (merge_plan) {
        return std::make_shared<MergePlan>(*merge_plan);
      }
    }
    return nullptr;
  }

  // 若存在超过 split_thresh 的分区且当前总分区数 < max_parts，则返回候选。
  std::optional<SplitPlan> GetSplitPlan() const;

  // 若存在低于 merge_thresh 的分区，则返回相邻两个合并候选。
  std::optional<MergePlan> GetMergePlan() const;

  enum class CompactionType : uint8_t { kNone = 0, kMerge = 1, kSplit = 2 };
  CompactionType NeedCompaction() const;
  double ComputeCompactionScore() const {
    return static_cast<double>(NeedCompaction());
  }

  void EncodeTo(std::string* dst) const;
  static bool DecodeFrom(const std::string_view& src,
                           std::shared_ptr<PartitionTable> table);

  std::shared_ptr<PartitionTable> ApplyToNewTable(const PartitionTableEdits &edits) const;

 private:
  void RebuildIndex() {
    growth_rate_index.clear();
    pid_index.clear();
    for (auto it = partition_storage.begin(); it != partition_storage.end(); ++it) {
      growth_rate_index.emplace(it->second.growth_rate, it);
      pid_index.emplace(it->second.partition_id, it);
    }
  }
  // Remove partition identified by pid.
  void RemovePartition(PartitionID pid);

  // Update the growth_rate of partition pid. Maintains index consistency.
  void UpdateGrowthRate(PartitionID pid, int64_t new_rate);
  PartitionTableStorage::iterator FindByPid(PartitionID pid);
  PartitionTableStorage::const_iterator FindByPid(PartitionID pid) const;

  PartitionInfo BuildPartitionInfo(
      PartitionTableStorage::const_iterator it) const;

  void EraseGrowthIndex(PartitionTableStorage::iterator pm_it);

  // 执行拆分：在 new_boundary 处创建新分区，将 pid 的 growth_rate 均分。
  void Split(PartitionID pid, const std::string& new_boundary);

  // 执行合并：删除 left_pid，将其 growth_rate 合并到 right_pid。
  void Merge(PartitionID left_pid, PartitionID right_pid);
};

class PartitionTableEdits {
 public:
  using Plan = PartitionTable::Plan;
  using SplitPlan = PartitionTable::SplitPlan;
  using MergePlan = PartitionTable::MergePlan;
  struct Edit {
    enum class Type : uint8_t { kSplit, kMerge, kPartitionUpdate, kInit };
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
    std::size_t key_count;
    PartitionUpdate(PartitionID pid, std::size_t key_count)
        : Edit(Type::kPartitionUpdate), pid(pid), key_count(key_count) {}
  };
  struct InitPartitionTable : Edit {
    std::shared_ptr<PartitionTable> pt;
    InitPartitionTable(std::shared_ptr<PartitionTable> pt)
        : Edit(Type::kInit), pt(std::move(pt)) {}
  };

  // Flush时迭代分区, 获取本次flush的Key数目, 并在edit_中记录分区表更新
  void UpdatePartitionKeyCount(PartitionID pid, std::size_t key_count) {
    edites_.push_back(std::make_shared<PartitionUpdate>(pid, key_count));
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

  auto GetEdits() const { return edites_; }

  // return "current" pt, which is const
  // auto GetPartitionTable() const { return pt_; }

 private:
  // const PartitionTable* pt_;
  std::list<std::shared_ptr<Edit>> edites_;
};

}  // namespace ROCKSDB_NAMESPACE
