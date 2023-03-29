//
// Created by 于承业 on 2023/3/22.
//

#include "table/value_block.h"

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "table/format.h"
#include "util/coding.h"

namespace leveldb {

ValueBlock::ValueBlock(const BlockContents& contents)
    : data_(contents.data.data()),
      size_(contents.data.size()),
      owned_(contents.heap_allocated)
{
  if(size_ < sizeof(uint32_t)){
    size_ = 0;
  } else {
    entry_index_offset_ = size_ - (1 + NumEntries()) * sizeof(uint32_t);
  }
}

ValueBlock::~ValueBlock() {
  if (owned_) {
    delete[] data_;
  }
}

uint32_t ValueBlock::NumEntries() const {
  assert(size_ >= sizeof(uint32_t));
  return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* key_len, uint32_t* value_len){
  if(limit - p < 2) return nullptr;
  *key_len = reinterpret_cast<const uint8_t*>(p)[0];
  if(*key_len < 128){
    p += 1;
  } else {
    if ((p = GetVarint32Ptr(p, limit, key_len)) == nullptr) return nullptr;
  }
  if ((p = GetVarint32Ptr(p, limit, value_len)) == nullptr) return nullptr;

  if (static_cast<uint32_t>(limit - p) < (*key_len + *value_len)) {
    return nullptr;
  }
  return p;
}

class ValueBlock::Iter : public Iterator {
 protected:
  const char* const data_;
  uint32_t current_;  // current offset of the block
  std::string key_;
  Slice value_;
  Status status_;

  uint32_t entry_index_;
  uint32_t entry_index_offset_;
  uint32_t num_entries_;

  uint32_t NextEntryOffset() const {
    return (value_.data() + value_.size()) - data_;
  }

  uint32_t GetEntryOffset(uint32_t index) const {
    assert(index < num_entries_);
    return DecodeFixed32(data_ + entry_index_offset_ + index * sizeof(uint32_t));
  }

  bool ok() const { return status().ok(); }

  bool ParseNextKey() {
    entry_index_++;
    current_ = NextEntryOffset();
    return ParseCurrentKey();
  }

  bool ParseCurrentKey() {
    const char* p = data_ + current_;
    const char* limit = data_ + entry_index_offset_;

    uint32_t key_len, value_len;
    p = DecodeEntry(p, limit, &key_len, &value_len);
    if(p == nullptr){
      CorruptionError();
      return false;
    } else {
      key_.assign(p, key_len);
      value_ = Slice(p + key_len, value_len);
      return true;
    }
    return false;
  }

  void CorruptionError(){
    status_ = Status::Corruption("bad entry in block");
    key_.clear();
    value_.clear();
  }

 public:
  Iter(const char* data,
       uint32_t entry_index_offset, uint32_t num_entries)
      : data_(data),
        entry_index_offset_(entry_index_offset),
        num_entries_(num_entries),
        current_(0),
        entry_index_(0)
  {}

  bool Valid() const override { return current_ < entry_index_offset_; }

  Status status() const override { return status_; }

  Slice key() const override {
    assert(Valid());
    return key_;
  }

  Slice value() const override {
    assert(Valid());
    return value_;
  }

  void SeekToFirst() override {
    entry_index_ = 0;
    current_ = GetEntryOffset(0);
    assert(Valid());
    ParseCurrentKey();
  }

  void SeekToLast() override {
    entry_index_ = num_entries_ - 1;
    current_ = GetEntryOffset(entry_index_);
    assert(Valid());
    ParseCurrentKey();
  }

  void Next() override {
    assert(Valid());
    ParseNextKey();
  }

  void Prev() override {
    assert(Valid());
    entry_index_--;
    current_ = GetEntryOffset(entry_index_);
    ParseCurrentKey();
  }
};

// ValueHandle => BlockHandle
class ValueBlock::IndexIter final: public ValueBlock::Iter {
 public:
  IndexIter(const char* data, uint32_t entry_index_offset, uint32_t num_entries)
      : ValueBlock::Iter(data, entry_index_offset, num_entries)
  {}

  // target is encoding of ValueHandle
  void Seek(const Slice& target) override {
    ValueHandle value_handle;
    Slice input = target;
    value_handle.DecodeFrom(&input);
    current_ = GetEntryOffset(value_handle.block_);
    ParseCurrentKey();
  }
};

// ValueHandle => block entry <key_len,val_len,key,value>
class ValueBlock::DataIter final: public ValueBlock::Iter {
 public:
  DataIter(const char* data, uint32_t entry_index_offset, uint32_t num_entries)
      : ValueBlock::Iter(data, entry_index_offset, num_entries)
  {}

  // target is encoding of ValueHandle
  void Seek(const Slice& target) override {
    ValueHandle value_handle;
    Slice input = target;
    value_handle.DecodeFrom(&input);
    current_ = GetEntryOffset(value_handle.offset_);
    ParseCurrentKey();
  }
};

Iterator* ValueBlock::NewIndexIterator(const ValueBlock* blk) {
  return new IndexIter(blk->data_, blk->entry_index_offset_,
                      blk->NumEntries());
}

Iterator* ValueBlock::NewDataIterator(const ValueBlock* blk) {
  return new DataIter(blk->data_, blk->entry_index_offset_,
                      blk->NumEntries());
}

ValueBlockBuilder::ValueBlockBuilder(const Options* options)
    : options_(options)
{

}

void ValueBlockBuilder::Reset() {
  buffer_.clear();
  entry_index_.clear();
  num_entries_ = 0;
  finished_ = false;
}

void ValueBlockBuilder::Add(const Slice& key, const Slice& value) {
  assert(!finished_);
  entry_index_.push_back(buffer_.size());

  PutVarint32(&buffer_, key.size());
  PutVarint32(&buffer_, value.size());
  buffer_.append(key.data(), key.size());
  buffer_.append(value.data(), value.size());

  num_entries_++;
}

Slice ValueBlockBuilder::Finish() {
  assert(num_entries_ == entry_index_.size());
  finished_ = true;
  for (uint32_t offset: entry_index_) {
    PutFixed32(&buffer_, offset);
  }
  PutFixed32(&buffer_, num_entries_);
  return {buffer_};
}

size_t ValueBlockBuilder::CurrentSizeEstimate() const {
  return buffer_.size() + kValueBlockFooterSize;
}
uint32_t ValueBlockBuilder::NumEntries() const { return num_entries_; }

}  // namespace leveldb