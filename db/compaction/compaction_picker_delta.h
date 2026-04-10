#pragma once
#ifndef ROCKSDB_LITE

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {

// lcr
// 动态分区Picker, Compaction时执行分区合并与拆分
class DeltaCompactionPicker : public CompactionPicker {
 public:
  DeltaCompactionPicker(const ImmutableOptions& ioptions,
                        const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}

  Compaction* PickCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer,
      SequenceNumber earliest_memtable_seqno = kMaxSequenceNumber) override;

  bool NeedsCompaction(const VersionStorageInfo* vstorage) const override;

  int MaxOutputLevel() const override { return 0; }
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
