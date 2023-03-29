//
// Created by 于承业 on 2023/3/29.
//

#ifndef LEVELDB_VLOG_H
#define LEVELDB_VLOG_H

#include "leveldb/options.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "table/format.h"
#include "util/coding.h"

namespace leveldb {

class VLogBuilder {
 public:
  VLogBuilder(const Options& options, AppendableRandomAccessFile* file);

  VLogBuilder(const VLogBuilder&) = delete;
  VLogBuilder& operator=(const VLogBuilder&) = delete;

  ~VLogBuilder();

  // Add a record to the file_ in append-only manner
  void Add(const Slice& key, const Slice& value);

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

  Iterator* NewIterator(const ReadOptions&) const;

  void IncreaseOffset(uint32_t new_offset);

 private:
  friend class VLogCache;
  class Iter;
  struct Rep;

  explicit VLogReader(Rep* rep): rep_(rep) {}

  Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                     void (*handle_result)(void* arg, const Slice& k,
                                           const Slice& v)) const;

  Rep* const rep_;
};

}  // namespace leveldb

#endif  // LEVELDB_VLOG_H
