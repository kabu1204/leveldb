//
// Created by 于承业 on 2023/3/29.
//
// Entry in the .vlog file:
//  key_len+8:  VarUint32
//  value_len:  VarUint32
//  key:        uint8[key_len]
//  SeqNum:     Fixed64
//  value:      uint8[value_len]

#include "table/vlog.h"

#include "db/value_batch.h"
#include "db/write_batch_internal.h"

namespace leveldb {

struct VLogBuilder::Rep {
  Rep(const Options& opt, AppendableRandomAccessFile* f)
      : options(opt), file(f) {}

  Options options;
  Status status;
  AppendableRandomAccessFile* file;
  bool closed{false};
  uint32_t num_entries{0};
  uint32_t offset{0};

  char buf[1024];
  std::string compressed;
};

VLogBuilder::VLogBuilder(const Options& options,
                         AppendableRandomAccessFile* file, bool reuse,
                         uint32_t offset, uint32_t num_entries)
    : rep_(new Rep(options, file))
{
  if (reuse) {
    rep_->offset = offset;
    rep_->num_entries = num_entries;
  }
}

VLogBuilder::~VLogBuilder() {
  assert(rep_->closed);
  delete rep_;
}

// TODO: should not be used
void VLogBuilder::Add(const Slice& key, const Slice& value, ValueHandle* handle) {
  assert(!rep_->closed);
  if(!ok()) return;
  uint32_t size = key.size() + value.size();
  // TODO: directly append to file
  char *buf = (size + 10) > 1024 ? new char[size+10]: rep_->buf;

  char *ptr = EncodeVarint32(buf, key.size());
  ptr = EncodeVarint32(ptr, value.size());
  size += (ptr - buf);
  std::memcpy(ptr, key.data(), key.size());
  std::memcpy(ptr + key.size(), value.data(), value.size());
  rep_->status = rep_->file->Append(Slice(buf, size));
  if(buf != rep_->buf){
    delete[] buf;
  }
  if(!ok()){
    return;
  }

  if (handle != nullptr) {
    handle->offset_ = rep_->offset;
    handle->size_ = size;
  }
  rep_->offset += size;
  rep_->num_entries++;
}

void VLogBuilder::AddBatch(const ValueBatch* batch) {
  rep_->status = rep_->file->Append(Slice(batch->data(), batch->size()));
  if (!ok()) return;

  rep_->offset += batch->size();
  rep_->num_entries += batch->NumEntries();
}

void VLogBuilder::Flush() {
  assert(!rep_->closed);
  rep_->status = rep_->file->Flush();
}

Status VLogBuilder::status() const { return rep_->status; }

Status VLogBuilder::Finish() {
  Flush();
  assert(!rep_->closed);
  rep_->closed = true;
  return rep_->status;
}

void VLogBuilder::Abandon() {
  assert(!rep_->closed);
  rep_->closed = true;
}

uint64_t VLogBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t VLogBuilder::FileSize() const { return rep_->offset; }

uint64_t VLogBuilder::Offset() const { return rep_->offset; }

struct VLogReader::Rep {
  Rep(const Options& opt, RandomAccessFile* f, uint32_t lim)
      : file(f),
        option(opt),
        limit(lim)
  {}

  Options option;
  Status status;
  uint32_t limit;
  RandomAccessFile* file;
};

static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* key_len, uint32_t* value_len) {
  if (limit - p < 2) return nullptr;
  *key_len = reinterpret_cast<const uint8_t*>(p)[0];
  if ((*key_len) < 128) {
    p++;
  } else {
    if ((p = GetVarint32Ptr(p, limit, key_len)) == nullptr) return nullptr;
  }
  if ((p = GetVarint32Ptr(p, limit, value_len)) == nullptr) return nullptr;

  if (static_cast<uint32_t>(limit - p) < (*key_len + *value_len)) {
    return nullptr;
  }
  return p;
}

// When file is appendable, file_size should be the actually len of the file.
Status VLogReader::Open(const Options& options, RandomAccessFile* file,
                        uint64_t file_size, VLogReader** reader) {
  if(reader != nullptr) {
    *reader = nullptr;
  }

  Rep *rep = new Rep(options, file, file_size);
  *reader = new VLogReader(rep);

  return Status::OK();
}

VLogReader::~VLogReader() {
  delete rep_;
}

class VLogReader::Iter : public Iterator {
 private:
  friend class VLogReader;
  const RandomAccessFile* const file_;
  const uint32_t limit_;
  uint32_t current_{0};
  Status status_;
  char buf_[1024];
  std::string key_;
  Slice value_;
  uint32_t parsed_entry_size_;
  bool valid_{false};

  bool ParseCurrentEntry(uint32_t size = 0){
    if(current_ >= limit_) return false;
    Slice result;
    char *scratch;
    const char* p;
    uint32_t key_len, val_len;

    if(size == 0){
      status_ = file_->Read(current_, std::min<size_t>(10, limit_-current_), &result, buf_);  // TODO: opt
      if (!status_.ok()) {
        return false;
      }
      p = DecodeEntry(result.data(), result.data() - current_ + limit_, &key_len, &val_len);
      if (p == nullptr) {
        status_ = Status::Corruption("corrupted vlog entry");
        return false;
      }

      size = key_len + val_len + (p - result.data());
      scratch = (key_len + val_len > sizeof(buf_)) ? new char[key_len + val_len] : buf_;
      status_ = file_->Read(current_ + (p - result.data()), key_len+val_len, &result, scratch);
      p = result.data();
    } else {
      scratch = (size > sizeof(buf_)) ? new char[size] : buf_;
      status_ = file_->Read(current_, size, &result, scratch);
      p = DecodeEntry(result.data(), result.data() + result.size(), &key_len, &val_len);
    }


    if(scratch != buf_) {
      delete[] scratch;
    }
    if(!status_.ok()){
      return false;
    }
    if(p == nullptr) {
      status_ = Status::Corruption("corrupted vlog entry");
      return false;
    }
    key_.assign(p, key_len);
    value_ = Slice(p + key_len, val_len);
    parsed_entry_size_ = size;
    return true;
  }

 public:
  Iter(const RandomAccessFile* file, uint32_t limit)
      : file_(file),
        limit_(limit) {}

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    current_ = 0;
    valid_ = ParseCurrentEntry();
  }

  void SeekToLast() override {
    uint32_t prev;
    if(current_ >= limit_) {
      SeekToFirst();
    }
    prev = current_;
    for(Next(); Valid(); Next()){
      prev = current_;
    }
    current_ = prev;
    valid_ = ParseCurrentEntry();
  }

  void Seek(const Slice& target) override {
    ValueHandle handle;
    Slice input = target;
    status_ = handle.DecodeFrom(&input);
    current_ = handle.offset_;
    valid_ = ParseCurrentEntry(handle.size_);
  }

  void Next() override {
    assert(current_ < limit_);
    current_ += parsed_entry_size_;
    valid_ = ParseCurrentEntry();
  }

  void Prev() override {
    status_ = Status::NotSupported("vlog iterator does not support iterating reversely");
    valid_ = false;
  }

  Slice key() const override { return {key_.data(), key_.size()}; }

  Slice value() const override { return value_; }

  Status status() const override { return status_; }
};

Iterator* VLogReader::NewIterator(const ReadOptions& option) const {
  return new Iter(rep_->file, rep_->limit);
}

void VLogReader::IncreaseOffset(uint32_t new_offset) {
  assert(new_offset >= rep_->limit);
  rep_->limit = new_offset;
}

Status VLogReader::InternalGet(const ReadOptions& option, const Slice& key, void* arg,
                               void (*handle_result)(void*, const Slice&,
                                                     const Slice&)) const {
  Status s;
  Iterator* iter = NewIterator(option);
  iter->Seek(key);
  if (iter->Valid()) {
    // TODO: do we need block cache here?
    (*handle_result)(arg, iter->key(), iter->value());
  }
  if (s.ok()) {
    s = iter->status();
  }
  delete iter;
  return s;
}

bool VLogReader::Validate(uint64_t* offset, uint64_t* num_entries) {
  Iter* iter = static_cast<Iter*>(NewIterator(ReadOptions()));
  assert(iter != nullptr);
  *offset = 0;
  *num_entries = 0;
  uint64_t n = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    n++;
  }
  *offset = iter->current_;
  *num_entries = n;
  delete iter;
  return *offset == rep_->limit;
}

// should be called after SetSequence()
void ValueBatch::Put(SequenceNumber s, const Slice& key, const Slice& value) {
  assert(!closed);
  uint32_t off = rep_.size();
  PutVarint32(&rep_, key.size() + sizeof(uint64_t));
  PutVarint32(&rep_, value.size());
  rep_.append(key.data(), key.size());
  PutFixed64(&rep_, s);
  rep_.append(value.data(), value.size());
  handles_.emplace_back(0, 0, off, rep_.size() - off);
  num_entries++;
}

Status ValueBatch::ToWriteBatch(WriteBatch* batch) {
  assert(closed);
  assert(num_entries == handles_.size());
  const char* p = rep_.data();
  const char* limit = rep_.data() + rep_.size();
  uint32_t found = 0;
  uint32_t key_len, val_len;
  SequenceNumber sequence;
  Slice key;
  std::string handle_encoding;

  while (p < limit) {
    if ((p = DecodeEntry(p, limit, &key_len, &val_len)) != nullptr) {
      key = Slice(p, key_len - sizeof(uint64_t));
      if (!found) {
        sequence = DecodeFixed64(p + key_len - sizeof(uint64_t));
      }
      assert(found == DecodeFixed64(p + key_len - sizeof(uint64_t)) - sequence);
      handles_[found].EncodeTo(&handle_encoding);
      WriteBatchInternal::Put(batch, key, Slice(handle_encoding),
                              kTypeValueHandle);
      found++;
      p += key_len + val_len;
    }
  }
  if (found != num_entries) {
    return Status::Corruption("corrupted ValueBatch");
  }
  WriteBatchInternal::SetSequence(batch, sequence);
  return Status::OK();
}

}  // namespace leveldb