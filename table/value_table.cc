//
// Created by 于承业 on 2023/3/22.
//

#include "table/value_table.h"

#include "leveldb/cache.h"
#include "leveldb/options.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "table/format.h"
#include "table/value_block.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct ValueTable::Rep {
  ~Rep() {
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  BlockHandle index_handle;
  ValueBlock* index_block;
};

Status ValueTable::Open(const Options& options, RandomAccessFile* file,
                        uint64_t file_size, ValueTable** vtable) {
  *vtable = nullptr;
  if (file_size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be a value table");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(file_size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if(!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if(!s.ok()) return s;

  BlockContents index_block_contents;
  ReadOptions opt;
  if(options.paranoid_checks){
    opt.verify_checksums = true;
  }
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if(s.ok()){
    ValueBlock* index_block = new ValueBlock(index_block_contents);
    Rep* rep = new ValueTable::Rep;
    rep->options = options;
    rep->file = file;
    rep->index_handle = footer.index_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    *vtable = new ValueTable(rep);
    (*vtable)->ReadMeta(footer);
  }

  return s;
}

ValueTable::~ValueTable() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<ValueBlock*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  ValueBlock* block = reinterpret_cast<ValueBlock*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

Iterator* ValueTable::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      ValueBlock::NewIndexIterator(rep_->index_block),
      &ValueTable::BlockReader,
      const_cast<ValueTable*>(this), options);
}

// Read a data block, return iterator
Iterator* ValueTable::BlockReader(void* arg, const ReadOptions& options,
                                  const Slice& index_value) {
  ValueTable* vtable = reinterpret_cast<ValueTable*>(arg);
  Cache* block_cache = vtable->rep_->options.block_cache;
  ValueBlock* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);

  if (s.ok()) {
    BlockContents contents;
    if(block_cache != nullptr){
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, vtable->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if(cache_handle != nullptr) {
        block = reinterpret_cast<ValueBlock*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(vtable->rep_->file, options, handle, &contents);
        if(s.ok()){
          block = new ValueBlock(contents);
          if(contents.cachable && options.fill_cache){
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               [](const Slice& key, void* value){
                                                 ValueBlock* block = reinterpret_cast<ValueBlock*>(value);
                                                 delete block;
                                               });
          }
        }
      }
    } else {
      s = ReadBlock(vtable->rep_->file, options, handle, &contents);
      if(s.ok()){
        block = new ValueBlock(contents);
      }
    }
  }

  Iterator* iter;
  if(block != nullptr){
    iter = ValueBlock::NewDataIterator(block);
    if(cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}



Status ValueTable::InternalGet(const ReadOptions& options, const Slice& key, void* arg,
                               void (*handle_result)(void*, const Slice&,
                                                     const Slice&)) {
  Status s;
  Iterator* iiter = ValueBlock::NewIndexIterator(rep_->index_block);
  iiter->Seek(key);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    BlockHandle handle;
    s = handle.DecodeFrom(&handle_value);
    if (s.ok()) {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(key);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    } else {
      return s;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

void ValueTable::ReadMeta(const Footer& footer) {}

struct ValueTableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
    : options(opt),
      file(f),
      index_block(&options),
      data_block(&options)
  {}

  Options options;
  WritableFile* file;
  ValueBlockBuilder index_block;  // store mappings: block_id => BlockHandle
  ValueBlockBuilder data_block;
  uint64_t offset{0};
  uint32_t num_entries{0};
  uint32_t num_data_blocks{0};
  Status status;
  bool closed{false};

  bool pending_handle{false};
  BlockHandle handle;

  std::string compressed;
};

ValueTableBuilder::ValueTableBuilder(const Options& options,
                                     WritableFile* file)
    : rep_(new Rep(options, file)){}

ValueTableBuilder::~ValueTableBuilder() {
  assert(rep_->closed);
  delete rep_;
}

void ValueTableBuilder::Add(const Slice& key, const Slice& value) {
  assert(!rep_->closed);
  if (!ok()) return;

  if (rep_->pending_handle) {
    std::string handle_encoding;
    std::string id_encoding;
    rep_->handle.EncodeTo(&handle_encoding);
    PutFixed32(&id_encoding, rep_->num_data_blocks - 1);
    rep_->index_block.Add(Slice(id_encoding), Slice(handle_encoding));
    rep_->pending_handle = false;
  }

  rep_->num_entries++;
  rep_->data_block.Add(key, value);

  if (rep_->data_block.CurrentSizeEstimate() >= rep_->options.block_size) {
    Flush();
  }
}

void ValueTableBuilder::Flush() {
  assert(!rep_->closed);
  if (!ok()) return;
  if (rep_->data_block.empty()) return;
  assert(!rep_->pending_handle);

  WriteBlock(&rep_->data_block, &rep_->handle);
  if (ok()) {
    rep_->pending_handle = true;
    rep_->status = rep_->file->Flush();
    rep_->num_data_blocks++;
  }
}

void ValueTableBuilder::WriteBlock(ValueBlockBuilder* block,
                                   BlockHandle* handle) {
  assert(ok());
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = rep_->options.compression;
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;
    case kSnappyCompression:
      std::string* compressed = &rep_->compressed;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        block_contents = raw;
        type = kNoCompression;
      }
      break;
  }
  WriteRawBlock(block_contents, type, handle);
  rep_->compressed.clear();
  block->Reset();
}

void ValueTableBuilder::WriteRawBlock(const Slice& data, CompressionType type,
                                      BlockHandle* handle) {
  handle->set_offset(rep_->offset);
  handle->set_size(data.size());
  rep_->status = rep_->file->Append(data);
  if (ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(data.data(), data.size());
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    rep_->status = rep_->file->Append(Slice(trailer, kBlockTrailerSize));
    if (ok()) {
      rep_->offset += data.size() + kBlockTrailerSize;
    }
  }
}

Status ValueTableBuilder::Finish() {
  Flush();
  assert(!rep_->closed);
  rep_->closed = true;

  BlockHandle index_handle;

  if (ok()) {
    if(rep_->pending_handle) {
      std::string handle_encoding;
      std::string id_encoding;
      rep_->handle.EncodeTo(&handle_encoding);
      PutFixed32(&id_encoding, rep_->num_data_blocks - 1);
      rep_->index_block.Add(Slice(id_encoding), Slice(handle_encoding));
      rep_->pending_handle = false;
    }
    WriteBlock(&rep_->index_block, &index_handle);
  }

  // Footer
  if (ok()) {
    Footer footer;
    std::string footer_encoding;
    footer.set_index_handle(index_handle);
    footer.EncodeTo(&footer_encoding);
    rep_->status = rep_->file->Append(footer_encoding);
    if(ok()){
      rep_->offset += footer_encoding.size();
    }
  }
  return status();
}

void ValueTableBuilder::Abandon() {
  assert(!rep_->closed);
  rep_->closed = true;
}

Status ValueTableBuilder::status() const { return rep_->status; }

uint64_t ValueTableBuilder::NumEntries() { return rep_->num_entries; }

uint64_t ValueTableBuilder::FileSize() { return rep_->offset; }

}  // namespace leveldb