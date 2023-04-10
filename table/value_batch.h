//
// Created by 于承业 on 2023/4/6.
//

#ifndef LEVELDB_VALUE_BATCH_H
#define LEVELDB_VALUE_BATCH_H

#include "db/dbformat.h"
#include <vector>

#include "leveldb/write_batch.h"

#include "table/format.h"

namespace leveldb {

class VLogBuilder;
class ValueLogImpl;

class ValueBatch {
 public:
  ValueBatch() : closed(false), num_entries(0) {}

  ~ValueBatch() = default;

  ValueBatch(const ValueBatch&) = default;
  ValueBatch& operator=(const ValueBatch&) = default;

  void Put(SequenceNumber seq, const Slice& key, const Slice& value);

  Status ToWriteBatch(WriteBatch* batch);

  uint32_t NumEntries() const { return num_entries; }

  const char* data() const { return rep_.data(); }

  size_t size() const { return rep_.size(); }

  static char* EncodeInternalKey(char* ptr, const Slice& user_key,
                                 uint64_t seq);

  static uint64_t DecodeInternalKey(const char* ptr, const char* end,
                                    Slice* user_key);

  static void PutInternalKey(std::string* dst, const Slice& user_key,
                             uint64_t seq);

  static bool GetInternalKeySeq(Slice* input, size_t n, Slice* user_key,
                                uint64_t* seq);

  static bool GetVLogRecord(Slice* input, Slice* user_key, Slice* value,
                            uint64_t* seq);

 private:
  friend class VLogBuilder;
  friend class ValueLogImpl;

  void Finalize(uint32_t table, uint32_t offset) {
    assert(!closed);
    closed = true;
    for (auto& handle : handles_) {
      handle.table_ = table;
      handle.offset_ += offset;
    }
  }

  bool closed;
  std::string rep_;
  uint32_t num_entries;
  std::vector<ValueHandle> handles_;
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_BATCH_H
