#include "db/partition_table.h"

#include <cassert>
#include <sstream>

namespace ROCKSDB_NAMESPACE {

PartitionTable::PartitionTableStorage::iterator PartitionTable::FindByPid(
    PartitionID pid) {
  auto it = pid_index.find(pid);
  return it == pid_index.end() ? partition_storage.end() : it->second;
}

PartitionTable::PartitionTableStorage::const_iterator PartitionTable::FindByPid(
    PartitionID pid) const {
  auto it = pid_index.find(pid);
  return it == pid_index.end() ? partition_storage.end() : it->second;
}

PartitionInfo PartitionTable::BuildPartitionInfo(
    PartitionTableStorage::const_iterator it) const {
  assert(it != partition_storage.end());
  PartitionInfo info = it->second;
  info.left_bound = it->first;
  auto next = std::next(it);
  info.right_bound =
      (next == partition_storage.end()) ? std::nullopt : next->first;
  return info;
}

void PartitionTable::EraseGrowthIndex(PartitionTableStorage::iterator pm_it) {
  int64_t rate = pm_it->second.stats.growth_rate;
  auto [gr_begin, gr_end] = growth_rate_index.equal_range(rate);
  for (auto gr_it = gr_begin; gr_it != gr_end; ++gr_it) {
    if (gr_it->second == pm_it) {
      growth_rate_index.erase(gr_it);
      return;
    }
  }
}

PartitionInfo PartitionTable::FindPartition(const std::string& user_key) const {
  assert(!partition_storage.empty());

  auto it = partition_storage.upper_bound(user_key);
  if (it == partition_storage.begin()) {
    return BuildPartitionInfo(it);
  }
  --it;
  return BuildPartitionInfo(it);
}

std::vector<PartitionID> PartitionTable::FindPartitionInRange(
    const std::optional<std::string>& left,
    const std::optional<std::string>& right) const {
  assert(!partition_storage.empty());

  // 确定起始迭代器：找到第一个与 [left, right) 有交集的分区
  PartitionTableStorage::const_iterator start_it;
  if (!left.has_value()) {
    start_it = partition_storage.begin();  // left = -INF，从第一个分区开始
  } else {
    start_it = partition_storage.upper_bound(left);
    // upper_bound 找到第一个 key > left 的位置
    // 自减后得到 key <= left 的最大分区，即覆盖 left 的分区
    if (start_it != partition_storage.begin()) {
      --start_it;
    } else {
      return {};  // left 比 map 中所有 key 都小，不应该发生（第一个分区 key 是 nullopt）
    }
  }

  // 确定结束迭代器：第一个 left_bound >= right 的分区
  PartitionTableStorage::const_iterator end_it;
  if (!right.has_value()) {
    end_it = partition_storage.end();  // right = +INF，遍历到最后
  } else {
    end_it = partition_storage.lower_bound(right);
    // lower_bound 找到第一个 key >= right 的位置
  }

  // 收集所有在 [start_it, end_it) 范围内的 PartitionID
  std::vector<PartitionID> pids;
  for (auto it = start_it; it != end_it; ++it) {
    pids.push_back(it->second.partition_id);
  }

  return pids;
}


PartitionID PartitionTable::InitFirstPartition() {
  if (!partition_storage.empty()) {
    return kInvalidPartitionID;
  }

  PartitionID pid = next_partition_id_++;
  PartitionInfo info;
  info.partition_id = pid;
  info.left_bound = std::nullopt;
  auto [it, inserted] =
      partition_storage.emplace(std::nullopt, std::move(info));
  assert(inserted);
  pid_index.emplace(pid, it);
  growth_rate_index.emplace(0, it);
  return pid;
}

PartitionID PartitionTable::AddPartition(const std::string& left_bound) {
  if (left_bound.empty() || partition_storage.empty() ||
      partition_storage.find(left_bound) != partition_storage.end()) {
    return kInvalidPartitionID;
  }

  PartitionID pid = next_partition_id_++;
  PartitionInfo info;
  info.partition_id = pid;
  info.left_bound = left_bound;
  auto [it, inserted] = partition_storage.emplace(left_bound, std::move(info));
  assert(inserted);
  pid_index.emplace(pid, it);
  growth_rate_index.emplace(0, it);
  return pid;
}

void PartitionTable::RemovePartition(PartitionID pid) {
  auto id_it = FindByPid(pid);
  assert(id_it != partition_storage.end());

  EraseGrowthIndex(id_it);
  pid_index.erase(pid);
  partition_storage.erase(id_it);
}

void PartitionTable::UpdateStats(PartitionID pid,
                                 const PartitionStats& new_stats) {
  auto id_it = FindByPid(pid);
  assert(id_it != partition_storage.end());
  EraseGrowthIndex(id_it);
  id_it->second.stats = new_stats;
  growth_rate_index.emplace(new_stats.growth_rate, id_it);
}

PartitionInfo PartitionTable::GetLowestGrowthPartition() const {
  assert(!growth_rate_index.empty());
  return growth_rate_index.begin()->second->second;
}

PartitionInfo PartitionTable::GetHighestGrowthPartition() const {
  assert(!growth_rate_index.empty());
  return growth_rate_index.rbegin()->second->second;
}

std::vector<PartitionID> PartitionTable::GetPartitions() const {
  std::vector<PartitionID> pids;
  pids.reserve(partition_storage.size());
  for (const auto& kv : partition_storage) {
    pids.push_back(kv.second.partition_id);
  }
  return pids;
}

std::vector<PartitionInfo> PartitionTable::GetPartitionInfos() const {
  std::vector<PartitionInfo> partitions;
  partitions.reserve(partition_storage.size());
  for (auto it = partition_storage.begin(); it != partition_storage.end();
       ++it) {
    partitions.push_back(BuildPartitionInfo(it));
  }
  return partitions;
}

std::string PartitionTable::DebugString() const {
  std::ostringstream oss;
  oss << "num_partitions=" << partition_storage.size()
      << " next_partition_id=" << next_partition_id_;

  if (partition_storage.empty()) {
    oss << " partitions=[]";
    return oss.str();
  }

  oss << " partitions=[";
  bool first = true;
  for (auto it = partition_storage.begin(); it != partition_storage.end();
       ++it) {
    if (!first) {
      oss << ", ";
    }
    first = false;

    const PartitionInfo info = BuildPartitionInfo(it);
    oss << "{pid=" << info.partition_id << " stats=" << info.stats.DebugString()
        << " range=["
        << (info.left_bound.has_value() ? info.left_bound.value()
                                        : std::string("-INF"))
        << ","
        << (info.right_bound.has_value() ? info.right_bound.value()
                                         : std::string("+INF"))
        << ")}";
  }
  oss << "]";
  return oss.str();
}

void PartitionTable::EncodeTo(std::string* dst) const {
  std::vector<char> buffer = struct_pack::serialize(*this);
  *dst = std::string(buffer.begin(), buffer.end());
}

bool PartitionTable::DecodeFrom(const std::string_view& src,
                                  std::shared_ptr<PartitionTable> table) {
  auto decoded = struct_pack::deserialize<PartitionTable>(src);
  if (!decoded.has_value()) {
    return false;
  }

  decoded->RebuildIndex();
  auto pt = decoded.value();
  *table = pt;
  return true;
}

std::optional<PartitionTable::SplitPlan> PartitionTable::GetSplitPlan(
    const std::unordered_set<PartitionID>& excluded) const {
  if (partition_storage.size() >= max_partitions_ || growth_rate_index.empty()) {
    return std::nullopt;
  }
  int64_t total = 0;
  for (const auto& kv : partition_storage) total += kv.second.stats.growth_rate;
  int64_t avg = total / static_cast<int64_t>(partition_storage.size());
  if (avg <= 0) return std::nullopt;

  // Scan from highest growth rate downward; skip partitions in excluded.
  for (auto it = growth_rate_index.rbegin(); it != growth_rate_index.rend(); ++it) {
    auto pm_it = it->second;
    PartitionID pid = pm_it->second.partition_id;
    if (excluded.count(pid) != 0) continue;
    if (static_cast<double>(it->first) <= avg * split_growth_threshold_) {
      // Remaining entries all below threshold.
      break;
    }
    SplitPlan plan;
    plan.pid = pid;
    plan.new_pid = next_partition_id_;
    return plan;
  }
  return std::nullopt;
}

void PartitionTable::Split(PartitionID pid, const std::string& new_boundary) {
  auto id_it = FindByPid(pid);
  assert(id_it != partition_storage.end());

  if (new_boundary.empty() ||
      partition_storage.find(new_boundary) != partition_storage.end()) {
    return;
  }

  const auto curr = id_it;
  const std::string old_lb =
      curr->first.has_value() ? curr->first.value() : std::string();
  if (new_boundary <= old_lb) {
    return;
  }
  auto next_it = std::next(curr);
  if (next_it != partition_storage.end() && next_it->first.has_value() &&
      new_boundary >= next_it->first.value()) {
    return;
  }

  PartitionStats old_stats = curr->second.stats;
  PartitionStats right_stats = old_stats * 0.5;
  PartitionStats left_stats = old_stats;
  left_stats -= right_stats;

  PartitionID new_pid = AddPartition(new_boundary);
  if (new_pid == kInvalidPartitionID) {
    return;
  }
  UpdateStats(pid, left_stats);
  UpdateStats(new_pid, right_stats);
}

std::optional<PartitionTable::MergePlan> PartitionTable::GetMergePlan(
    const std::unordered_set<PartitionID>& excluded) const {
  if (partition_storage.size() < 2 || growth_rate_index.empty()) {
    return std::nullopt;
  }
  int64_t total = 0;
  for (const auto& kv : partition_storage) total += kv.second.stats.growth_rate;
  int64_t avg = total / static_cast<int64_t>(partition_storage.size());
  if (avg <= 0) return std::nullopt;

  // Scan from lowest growth rate upward; skip excluded candidates.
  for (auto lo_it = growth_rate_index.begin();
       lo_it != growth_rate_index.end(); ++lo_it) {
    if (static_cast<double>(lo_it->first) >= avg * merge_growth_threshold_) {
      // Remaining entries all above threshold.
      break;
    }
    auto merge_pm_it = lo_it->second;
    PartitionID candidate_pid = merge_pm_it->second.partition_id;
    if (excluded.count(candidate_pid) != 0) continue;

    // Try right neighbor first.
    auto next_it = std::next(merge_pm_it);
    if (next_it != partition_storage.end()) {
      PartitionID right_pid = next_it->second.partition_id;
      if (excluded.count(right_pid) == 0) {
        MergePlan plan;
        plan.left_pid = candidate_pid;
        plan.right_pid = right_pid;
        return plan;
      }
    }
    // Try left neighbor.
    if (merge_pm_it != partition_storage.begin()) {
      auto prev_it = std::prev(merge_pm_it);
      PartitionID left_pid = prev_it->second.partition_id;
      if (excluded.count(left_pid) == 0) {
        MergePlan plan;
        plan.left_pid = left_pid;
        plan.right_pid = candidate_pid;
        return plan;
      }
    }
    // Both neighbors excluded; try next low-growth candidate.
  }
  return std::nullopt;
}

void PartitionTable::Merge(PartitionID left_pid, PartitionID right_pid) {
  auto left_it = FindByPid(left_pid);
  auto right_it = FindByPid(right_pid);
  assert(left_it != partition_storage.end());
  assert(right_it != partition_storage.end());
  assert(std::next(left_it) == right_it);

  PartitionStats merged_stats = right_it->second.stats;
  merged_stats += left_it->second.stats;

  const auto new_left = left_it->first;

  EraseGrowthIndex(left_it);
  EraseGrowthIndex(right_it);

  pid_index.erase(left_pid);

  auto right_node = partition_storage.extract(right_it);
  right_node.key() = new_left;
  right_node.mapped().left_bound = new_left;
  right_node.mapped().stats = merged_stats;

  partition_storage.erase(left_it);

  auto insert_result = partition_storage.insert(std::move(right_node));
  auto new_it = insert_result.position;
  pid_index[right_pid] = new_it;
  growth_rate_index.emplace(merged_stats.growth_rate, new_it);
}

PartitionTable::CompactionType PartitionTable::NeedCompaction(
    const std::unordered_set<PartitionID>& excluded) const {
  if (file_num_compaction_trigger_ !=
      std::numeric_limits<uint32_t>::max()) {
    for (const auto& kv : partition_storage) {
      if (excluded.count(kv.second.partition_id) == 0 &&
              static_cast<int64_t>(file_num_compaction_trigger_)) {
        return CompactionType::kPartition;
      }
    }
  }

  if (NumPartitions() < 2) {
    return CompactionType::kNone;
  }
  if (GetSplitPlan(excluded).has_value()) {
    return CompactionType::kSplit;
  }
  if (GetMergePlan(excluded).has_value()) {
    return CompactionType::kMerge;
  }
  return CompactionType::kNone;
}

std::optional<PartitionTable::PartitionCompactionPlan>
PartitionTable::GetPartitionCompactionPlan(
    const std::unordered_set<PartitionID>& excluded) const {
  if (file_num_compaction_trigger_ ==
          std::numeric_limits<uint32_t>::max() ||
      partition_storage.empty()) {
    return std::nullopt;
  }
  const int64_t threshold =
      static_cast<int64_t>(file_num_compaction_trigger_);
  PartitionID best_pid = kInvalidPartitionID;
  int64_t best_count = threshold - 1;  // must exceed threshold
  for (const auto& kv : partition_storage) {
    if (excluded.count(kv.second.partition_id) != 0) continue;
    if (kv.second.stats.file_count > best_count) {
      best_count = kv.second.stats.file_count;
      best_pid = kv.second.partition_id;
    }
  }
  if (best_pid == kInvalidPartitionID) {
    return std::nullopt;
  }
  PartitionCompactionPlan plan;
  plan.pid = best_pid;
  return plan;
}

std::shared_ptr<PartitionTable> PartitionTable::ApplyToNewTable(
    const PartitionTableEdits& pt_edits) const {
  auto new_pt = std::make_shared<PartitionTable>(*this);
  for (const auto& edit : pt_edits.GetEdits()) {
    if (edit->type == PartitionTableEdits::Edit::Type::kSplit) {
      auto split = std::static_pointer_cast<PartitionTableEdits::Split>(edit);
      const auto& plan = split->plan;
      new_pt->Split(plan.pid, split->new_boundary);
    } else if (edit->type == PartitionTableEdits::Edit::Type::kMerge) {
      auto merge = std::static_pointer_cast<PartitionTableEdits::Merge>(edit);
      const auto& plan = merge->plan;
      new_pt->Merge(plan.left_pid, plan.right_pid);
    } else if (edit->type == PartitionTableEdits::Edit::Type::kPartitionUpdate) {
      auto update =
          std::static_pointer_cast<PartitionTableEdits::PartitionUpdate>(edit);
      PartitionID pid = update->pid;
      // 此时可能已经Compaction了, pid可能不存在
      auto partition = new_pt->GetPartition(pid);
      if (partition != std::nullopt) {
        PartitionStats new_stats = partition->stats;
        new_stats += update->stats;
        new_pt->UpdateStats(pid, new_stats);
      }
    } else if (edit->type == PartitionTableEdits::Edit::Type::kInit) {
      auto init =
          std::static_pointer_cast<PartitionTableEdits::InitPartitionTable>(edit);
      new_pt = init->pt;
    }
  }
  // edites.clear();
  return new_pt;
};

}  // namespace ROCKSDB_NAMESPACE
