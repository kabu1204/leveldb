//
// Created by 于承业 on 2023/3/22.
//

#ifndef LEVELDB_VALUE_TABLE_H
#define LEVELDB_VALUE_TABLE_H

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/status.h"
#include "leveldb/options.h"
#include "leveldb/iterator.h"
#include "table/value_block.h"
#include "table/format.h"
#include "util/coding.h"

namespace leveldb {

class WritableFile;
class RandomAccessFile;
class TableCache;

/*
 * ValueTable is a map from <block, offset> to strings.
 */
class ValueTable {
 public:
  static Status Open(const Options& options, RandomAccessFile* file,
                     uint64_t file_size, ValueTable** vtable);

  ValueTable(const ValueTable&) = delete;
  ValueTable& operator=(const ValueTable&) = delete;

  ~ValueTable();

  Iterator* NewIterator(const ReadOptions&) const;

 private:
  friend class ValueTableCache;
  struct Rep;

  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  explicit  ValueTable(Rep* rep): rep_(rep) {}

  Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                     void (*handle_result)(void* arg, const Slice& k,
                                           const Slice& v));

  void ReadMeta(const Footer& footer);

  Rep* const rep_;
};

class ValueTableBuilder {
 public:
  ValueTableBuilder(const Options& options, WritableFile* file);

  ValueTableBuilder(const ValueTableBuilder&) = delete;
  ValueTableBuilder& operator=(const ValueTableBuilder&) = delete;

  ~ValueTableBuilder();

  // Add a record to the file_ in append-only manner
  void Add(const Slice& key, const Slice& value);

  void Flush();

  Status status() const;

  Status Finish();

  void Abandon();

  uint64_t NumEntries();

  uint64_t FileSize();

 private:
  bool ok() const {return status().ok(); }
  void WriteBlock(ValueBlockBuilder* block, BlockHandle* handle);
  void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

  struct Rep;
  Rep* rep_;  // internal representation
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_TABLE_H
