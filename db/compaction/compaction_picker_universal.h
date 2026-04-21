//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <memory>
#ifndef ROCKSDB_LITE

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {
class UniversalCompactionPicker : public CompactionPicker {
 public:
  UniversalCompactionPicker(const ImmutableOptions& ioptions,
                            const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}
  virtual Compaction* PickCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer,
      SequenceNumber earliest_memtable_seqno = kMaxSequenceNumber) override;
  virtual int MaxOutputLevel() const override { return NumberLevels() - 1; }

  virtual bool NeedsCompaction(
      const VersionStorageInfo* vstorage) const override;
};

namespace ROCKSDB_UNIVERSAL_COMPACTION_BUILDER {

// A helper class that form universal compactions. The class is used by
// UniversalCompactionPicker::PickCompaction().
// The usage is to create the class, and get the compaction object by calling
// PickCompaction().
class UniversalCompactionBuilder {
 public:
  UniversalCompactionBuilder(
      const ImmutableOptions& ioptions, const InternalKeyComparator* icmp,
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options, std::shared_ptr<VersionStorageInfoView> vstorage_view,
      CompactionPicker* picker, LogBuffer* log_buffer)
      : ioptions_(ioptions),
        icmp_(icmp),
        cf_name_(cf_name),
        mutable_cf_options_(mutable_cf_options),
        mutable_db_options_(mutable_db_options),
        vstorage_(vstorage_view),
        picker_(picker),
        log_buffer_(log_buffer) {}

  // Form and return the compaction object. The caller owns return object.
  Compaction* PickCompaction();

 private:
  struct SortedRun {
    SortedRun(int _level, FileMetaData* _file, uint64_t _size,
              uint64_t _compensated_file_size, bool _being_compacted)
        : level(_level),
          file(_file),
          size(_size),
          compensated_file_size(_compensated_file_size),
          being_compacted(_being_compacted) {
      assert(compensated_file_size > 0);
      assert(level != 0 || file != nullptr);
    }

    void Dump(char* out_buf, size_t out_buf_size,
              bool print_path = false) const;

    // sorted_run_count is added into the string to print
    void DumpSizeInfo(char* out_buf, size_t out_buf_size,
                      size_t sorted_run_count) const;

    int level;
    // `file` Will be null for level > 0. For level = 0, the sorted run is
    // for this file.
    FileMetaData* file;
    // For level > 0, `size` and `compensated_file_size` are sum of sizes all
    // files in the level. `being_compacted` should be the same for all files
    // in a non-zero level. Use the value here.
    uint64_t size;
    uint64_t compensated_file_size;
    bool being_compacted;
  };

  // Pick Universal compaction to limit read amplification
  Compaction* PickCompactionToReduceSortedRuns(
      unsigned int ratio, unsigned int max_number_of_files_to_compact);

  // Pick Universal compaction to limit space amplification.
  Compaction* PickCompactionToReduceSizeAmp();

  // Try to pick incremental compaction to reduce space amplification.
  // It will return null if it cannot find a fanout within the threshold.
  // Fanout is defined as
  //    total size of files to compact at output level
  //  --------------------------------------------------
  //    total size of files to compact at other levels
  Compaction* PickIncrementalForReduceSizeAmp(double fanout_threshold);

  Compaction* PickDeleteTriggeredCompaction();

  // Form a compaction from the sorted run indicated by start_index to the
  // oldest sorted run.
  // The caller is responsible for making sure that those files are not in
  // compaction.
  Compaction* PickCompactionToOldest(size_t start_index,
                                     CompactionReason compaction_reason);

  Compaction* PickCompactionWithSortedRunRange(
      size_t start_index, size_t end_index, CompactionReason compaction_reason);

  // Try to pick periodic compaction. The caller should only call it
  // if there is at least one file marked for periodic compaction.
  // null will be returned if no such a compaction can be formed
  // because some files are being compacted.
  Compaction* PickPeriodicCompaction();

  // Used in universal compaction when the allow_trivial_move
  // option is set. Checks whether there are any overlapping files
  // in the input. Returns true if the input files are non
  // overlapping.
  bool IsInputFilesNonOverlapping(Compaction* c);

  uint64_t GetMaxOverlappingBytes() const;

  const ImmutableOptions& ioptions_;
  const InternalKeyComparator* icmp_;
  double score_;
  std::vector<SortedRun> sorted_runs_;
  const std::string& cf_name_;
  const MutableCFOptions& mutable_cf_options_;
  const MutableDBOptions& mutable_db_options_;
  std::shared_ptr<VersionStorageInfoView> vstorage_;
  CompactionPicker* picker_;
  LogBuffer* log_buffer_;

  static std::vector<SortedRun> CalculateSortedRuns(
      const VersionStorageInfoView& vstorage);

  // Pick a path ID to place a newly generated file, with its estimated file
  // size.
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            uint64_t file_size);
};

} // namespace ROCKSDB_UNIVERSAL_COMPACTION_BUILDER

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
