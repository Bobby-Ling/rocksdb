// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/compaction/compaction_picker_delta.h"
#ifndef ROCKSDB_LITE

#include <memory>
#include <string>
#include <vector>

#include "db/compaction/compaction_picker_universal.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

using ROCKSDB_UNIVERSAL_COMPACTION_BUILDER::UniversalCompactionBuilder;

std::unordered_set<PartitionID> DeltaCompactionPicker::CollectBusyPartitions()
    const {
  std::unordered_set<PartitionID> busy;
  for (const Compaction* c : compactions_in_progress_) {
    const auto& plan = c->GetCompactionPlan();
    if (!plan) continue;
    auto plan_busy = plan->GetBusyPartitions();
    busy.insert(plan_busy.begin(), plan_busy.end());
    auto plan_out = plan->GetOutPartitions();
    busy.insert(plan_out.begin(), plan_out.end());
  }
  return busy;
}

bool DeltaCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  // Score is set by VersionStorageInfo::ComputeCompactionScore for Delta style.
  auto pt = vstorage->GetPartitionTable();
  if (!pt || !pt->IsInitialized()) {
    return false;
  }
  auto busy = CollectBusyPartitions();
  bool needs_compaction =
      pt->NeedCompaction(busy) != PartitionTable::CompactionType::kNone;
  ROCKS_LOG_INFO(ioptions_.info_log,
                 "DeltaCompactionPicker::NeedsCompaction() returns %d",
                 needs_compaction);
  return needs_compaction;
}

Compaction* DeltaCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, SequenceNumber /*earliest_memtable_seqno*/) {
  (void)cf_name;
  (void)log_buffer;
  const auto& pt = vstorage->GetPartitionTable();
  if (!pt || !pt->IsInitialized()) {
    return nullptr;
  }

  auto plan = pt->GetCompactionPlan(CollectBusyPartitions());
  if (plan == nullptr) {
    return nullptr;
  }
  std::unordered_set<PartitionID> partition_ids;
  int target_partition_count = 0;
  CompactionReason reason;
  if (plan->type == PartitionTable::Plan::Type::kSplit) {
    auto split_plan = std::static_pointer_cast<PartitionTable::SplitPlan>(plan);
    partition_ids.insert(split_plan->pid);
    target_partition_count = 2;
    reason = CompactionReason::kDeltaSplit;
  } else if (plan->type == PartitionTable::Plan::Type::kMerge) {
    auto merge_plan = std::static_pointer_cast<PartitionTable::MergePlan>(plan);
    partition_ids.insert(merge_plan->left_pid);
    partition_ids.insert(merge_plan->right_pid);
    target_partition_count = 1;
    reason = CompactionReason::kDeltaMerge;
  } else if (plan->type == PartitionTable::Plan::Type::kPartition) {
    auto partition_plan =
        std::static_pointer_cast<PartitionTable::PartitionCompactionPlan>(plan);
    partition_ids.insert(partition_plan->pid);
    target_partition_count = 1;
    reason = CompactionReason::kDeltaPartition;
  } else if (plan->type == PartitionTable::Plan::Type::kRangeDelete) {
    auto rd_plan =
        std::static_pointer_cast<PartitionTable::RangeDeleteCompactionPlan>(
            plan);
    partition_ids.insert(rd_plan->pid);
    target_partition_count = 1;
    reason = CompactionReason::kDeltaRangeDelete;
  } else {
    assert(false);
    return nullptr;
  }

  auto vstorage_view = std::make_shared<VersionStorageInfoViewDelta>(
      vstorage, std::move(partition_ids));
  const auto& files = vstorage_view->LevelFiles(0);
  if (files.empty() || AreFilesInCompaction(files)) {
    return nullptr;
  }

  if (reason == CompactionReason::kDeltaSplit ||
      reason == CompactionReason::kDeltaMerge) {
    std::vector<CompactionInputFiles> comp_inputs(1);
    comp_inputs[0].level = 0;
    comp_inputs[0].files = files;

    const int output_level = 0;
    const uint64_t target_file_size = mutable_cf_options.target_file_size_base;
    const uint64_t max_compaction_bytes =
        mutable_cf_options.max_compaction_bytes;
    const uint32_t output_path_id = 0;
    const CompressionType compression = GetCompressionType(
        vstorage, mutable_cf_options, output_level, vstorage->base_level());
    const CompressionOptions compression_opts =
        GetCompressionOptions(mutable_cf_options, vstorage, output_level);

    auto* c = new Compaction(
        vstorage_view, ioptions_, mutable_cf_options, mutable_db_options,
        std::move(comp_inputs), output_level, target_file_size,
        max_compaction_bytes, output_path_id, compression, compression_opts,
        Temperature::kUnknown, target_partition_count,
        {}, false /* manual_compaction */, "",
        vstorage_view->CompactionScore(0), false /* deletion_compaction */, true,
        reason, BlobGarbageCollectionPolicy::kUseDefault, -1, plan);

    RegisterCompaction(c);
    ROCKS_LOG_INFO(ioptions_.info_log,
                   "Picked Delta Compaction for partition plan: %s",
                   plan->DebugString().c_str());
    return c;
  }

  // For RangeDelete compaction.
  MutableCFOptions overrided_mutable_cf_options = mutable_cf_options;
  if (reason == CompactionReason::kDeltaRangeDelete) {
    overrided_mutable_cf_options.level0_file_num_compaction_trigger = 1;
  }

  UniversalCompactionBuilder builder(
      ioptions_, icmp(), cf_name, overrided_mutable_cf_options, mutable_db_options,
      std::move(vstorage_view), this, log_buffer);

  auto* c = builder.PickCompaction();
  if (c == nullptr) {
    return nullptr;
  }

  c->SetCompactionReason(reason);
  c->SetMaxSubcompactions(target_partition_count);
  c->SetCompactionPlan(plan);

  ROCKS_LOG_INFO(ioptions_.info_log,
                 "Picked Delta Compaction via Universal builder for partition plan: %s",
                 plan->DebugString().c_str());

  return c;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
