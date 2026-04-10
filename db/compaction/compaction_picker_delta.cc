// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/compaction/compaction_picker_delta.h"
#ifndef ROCKSDB_LITE

#include <algorithm>
#include <string>
#include <vector>

#include "logging/log_buffer.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

bool DeltaCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  // Score is set by VersionStorageInfo::ComputeCompactionScore for Delta style.
  return vstorage->CompactionScore(0) >= 1;
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

  auto plan = pt->GetCompactionPlan();
  if (plan == nullptr) {
    return nullptr;
  }
  std::vector<FileMetaData*> files;
  int target_partition_count = 0;
  if (plan->type == PartitionTable::Plan::Type::kSplit) {
    auto split_plan = std::static_pointer_cast<PartitionTable::SplitPlan>(plan);
    files = vstorage->GetFilesInPartition(split_plan->pid);
    target_partition_count = 2;
  } else if (plan->type == PartitionTable::Plan::Type::kMerge) {
    auto merge_plan = std::static_pointer_cast<PartitionTable::MergePlan>(plan);
    auto left_files = vstorage->GetFilesInPartition(merge_plan->left_pid);
    auto right_files = vstorage->GetFilesInPartition(merge_plan->right_pid);
    files.reserve(left_files.size() + right_files.size());
    files.insert(files.end(), left_files.begin(), left_files.end());
    files.insert(files.end(), right_files.begin(), right_files.end());
    target_partition_count = 1;
  } else {
    assert(false);
    return nullptr;
  }

  if (files.empty() || AreFilesInCompaction(files)) {
    return nullptr;
  }

  std::sort(files.begin(), files.end(),
            [&](const FileMetaData* a, const FileMetaData* b) {
              return icmp()->Compare(a->smallest, b->smallest) < 0;
            });

  CompactionInputFiles start_level_inputs;
  start_level_inputs.level = 0;
  start_level_inputs.files = std::move(files);

  std::vector<CompactionInputFiles> comp_inputs;
  comp_inputs.push_back(std::move(start_level_inputs));

  const int output_level = 0;
  const uint64_t target_file_size = mutable_cf_options.target_file_size_base;
  const uint64_t max_compaction_bytes = mutable_cf_options.max_compaction_bytes;
  const uint32_t output_path_id = 0;
  const CompressionType compression =
      GetCompressionType(vstorage, mutable_cf_options, output_level,
                         vstorage->base_level());
  const CompressionOptions compression_opts =
      GetCompressionOptions(mutable_cf_options, vstorage, output_level);

  auto* c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(comp_inputs), output_level, target_file_size,
      max_compaction_bytes, output_path_id, compression, compression_opts,
      Temperature::kUnknown,
      target_partition_count,
      std::vector<FileMetaData*>(), false /* manual_compaction */, "",
      vstorage->CompactionScore(0), false /* deletion_compaction */, true,
      CompactionReason::kUnknown, BlobGarbageCollectionPolicy::kUseDefault,
      -1, plan);

  RegisterCompaction(c);

  ROCKS_LOG_INFO(ioptions_.info_log, "Picked Delta Compaction for partition plan: %u",
                 static_cast<uint32_t>(plan->type));

  return c;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
