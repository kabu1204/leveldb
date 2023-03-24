//
// Created by 于承业 on 2023/3/23.
//

#include "db/value_table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "table/value_table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  ValueTable* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

ValueTableCache::ValueTableCache(const std::string& dbname,
                                 const Options& options, int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries))
{}

ValueTableCache::~ValueTableCache() { delete cache_; }

Iterator* ValueTableCache::NewIterator(const ReadOptions& options,
                                       uint64_t file_number, uint64_t file_size,
                                       ValueTable** tableptr) {
  if(tableptr != nullptr){
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if(!s.ok()){
    return NewErrorIterator(s);
  }

  ValueTable* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if(tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status ValueTableCache::Get(const ReadOptions& options, uint64_t file_number,
                            uint64_t file_size, const Slice& k, void* arg,
                            void (*handle_result)(void*, const Slice&,
                                                  const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    ValueTable* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);
  }
  return s;
}

void ValueTableCache::Evict(uint64_t file_number) {
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    cache_->Erase(Slice(buf, sizeof(buf)));
}

Status ValueTableCache::FindTable(uint64_t file_number, uint64_t file_size,
                                  Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if(*handle == nullptr) {
    std::string fname = ValueTableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    ValueTable* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if(s.ok()){
      s = ValueTable::Open(options_, file, file_size, &table);
    }

    if(!s.ok()) {
      assert(table == nullptr);
      delete file;
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }

  return s;
}
}  // namespace leveldb