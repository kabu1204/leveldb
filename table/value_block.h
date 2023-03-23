//
// Created by 于承业 on 2023/3/22.
//

#ifndef LEVELDB_VALUE_BLOCK_H
#define LEVELDB_VALUE_BLOCK_H

#include <vector>
#include <cstddef>
#include <cstdint>

#include "leveldb/status.h"
#include "leveldb/options.h"
#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;

class ValueBlock {
 public:
  explicit ValueBlock(const BlockContents& contents);

  ValueBlock(const ValueBlock&) = delete;
  ValueBlock& operator=(const ValueBlock&) = delete;

  ~ValueBlock();


  size_t size() const { return size_; }
  static Iterator* NewDataIterator(const ValueBlock* blk);
  static Iterator* NewIndexIterator(const ValueBlock* blk);

 private:
  class Iter;
  class IndexIter;
  class DataIter;

  uint32_t NumEntries() const;

  uint32_t entry_index_offset_;  // Offset in data_ of the entry_index_ array

  const char* data_;
  size_t size_;
  bool owned_;               // Block owns data_[]
};

class ValueBlockBuilder {
 public:

  enum { kValueBlockFooterSize = sizeof(uint32_t) };  // num_entries

  explicit ValueBlockBuilder(const Options* options);

  ValueBlockBuilder(const ValueBlockBuilder&) = delete;
  ValueBlockBuilder& operator=(const ValueBlockBuilder&) = delete;

  ~ValueBlockBuilder() = default;

  void Reset();

  /*
   * In order to check the validity of the value, we need to
   * keep the key.
   * There's no order requirements for the key.
   */
  void Add(const Slice& key, const Slice& value);

  Slice Finish();

  size_t CurrentSizeEstimate() const;

  uint32_t NumEntries() const;

  bool empty() const { return buffer_.empty(); }

 private:
  const Options* options_;
  std::string buffer_;
  uint32_t num_entries_{0};
  bool finished_{false};

  // entry_index_[i] is the i-th entry's offset in the block
  std::vector<uint32_t> entry_index_;
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_BLOCK_H
