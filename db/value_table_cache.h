//
// Created by 于承业 on 2023/3/23.
//

#ifndef LEVELDB_VALUE_TABLE_CACHE_H
#define LEVELDB_VALUE_TABLE_CACHE_H

#include <cstdint>
#include <string>

#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "table/vlog.h"
#include "port/port.h"

namespace leveldb {

class Env;

class VLogCache {
 public:
  VLogCache(const std::string& dbname, const Options& options, int entries);

  VLogCache(const VLogCache&) = delete;
  VLogCache& operator=(const VLogCache&) = delete;

  ~VLogCache();

  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, VLogReader** reader = nullptr);

  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

 private:
  Status FindReader(uint64_t file_number, uint64_t file_size, Cache::Handle** handle);

  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  Cache* cache_;
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_TABLE_CACHE_H
