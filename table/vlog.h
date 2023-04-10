//
// Created by 于承业 on 2023/3/29.
//

#ifndef LEVELDB_VLOG_H
#define LEVELDB_VLOG_H

#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"

#include "table/format.h"
#include "table/value_batch.h"
#include "table/vlog_iter.h"
#include "util/coding.h"

namespace leveldb {

class VLogBuilder {
 public:
  VLogBuilder(const Options& options, AppendableRandomAccessFile* file,
              bool reuse = false, uint32_t offset = 0,
              uint32_t num_entries = 0);

  VLogBuilder(const VLogBuilder&) = delete;
  VLogBuilder& operator=(const VLogBuilder&) = delete;

  ~VLogBuilder();

  // Add a record to the file_ in append-only manner
  void Add(const Slice& key, const Slice& value, ValueHandle* handle);

  void AddBatch(const ValueBatch* batch);

  void Flush();

  Status status() const;

  Status Finish();

  void Abandon();

  uint64_t NumEntries() const;

  uint64_t FileSize() const;

  uint64_t Offset() const;

 private:
  bool ok() const {return status().ok(); }

  struct Rep;
  Rep* rep_;
};

class VLogReader {
 public:
  static Status Open(const Options& options, RandomAccessFile* file,
                     uint64_t file_size, VLogReader** reader);

  VLogReader(const VLogReader&) = delete;
  VLogReader& operator=(const VLogReader&) = delete;

  ~VLogReader();

  VLogReaderIterator* NewIterator(const ReadOptions&) const;

  /*
   * Iterate through the file, stop when !iter->Valid(),
   * storing the validated size and num_entries of the file in the arguments.
   * return false when *offset != fileSize.
   */
  bool Validate(uint64_t* offset, uint64_t* num_entries);

  void IncreaseOffset(uint32_t new_offset);

 private:
  friend class VLogCache;
  struct Rep;

  explicit VLogReader(Rep* rep): rep_(rep) {}

  Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                     void (*handle_result)(void* arg, const Slice& k,
                                           const Slice& v)) const;

  Rep* const rep_;
};

}  // namespace leveldb

#endif  // LEVELDB_VLOG_H
