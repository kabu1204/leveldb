//
// Created by 于承业 on 2023/3/22.
//

#ifndef LEVELDB_VALUE_LOG_H
#define LEVELDB_VALUE_LOG_H

#include <vector>
#include <deque>
#include <set>
#include <map>

#include "db/value_table_cache.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/env.h"
#include "table/format.h"
#include "table/vlog.h"
#include "port/port.h"

namespace leveldb {

struct VLogFileMeta {
  VLogFileMeta(){}

  int refs{0};
  uint64_t number;
  uint64_t file_size{0};
};

// thread-safe
class ValueLog {
 public:
  explicit ValueLog(const Options& options, const std::string& dbname);

  ~ValueLog();

  ValueLog(const ValueLog&) = delete;
  ValueLog& operator=(const ValueLog&) = delete;

  Status Add(const WriteOptions& options, const Slice& key, const Slice& value,
                     ValueHandle* handle);

  Status Get(const ReadOptions& options, const ValueHandle& handle, std::string* value);

  Status Recover();

 private:
  uint64_t NewFileNumber() { return ++vlog_file_number_; }
  uint64_t CurrentFileNumber() const { return vlog_file_number_; }
  void ReuseFileNumber(uint64_t number) {
    if (number == CurrentFileNumber() + 1){
      vlog_file_number_ = number;
    }
  }

  Status FinishBuild();
  Status NewVLogBuider();

  struct ByFileNumber {
    bool operator()(VLogFileMeta* lhs, VLogFileMeta* rhs) const {
      return lhs->number < rhs->number;
    }
  };

  port::Mutex mutex_;
  Options options_;
  Env* const env_;
  const std::string dbname_;
  std::atomic<bool> shutdown_{false};

  AppendableRandomAccessFile* rw_file_; // the file that currently being built
  VLogBuilder* builder_;  // associated with rw_file_
  VLogReader* reader_;    // associated with rw_file_

//  std::set<VLogFileMeta*, ByFileNumber> ro_files_;
  std::map<uint64_t, VLogFileMeta*> ro_files_;  // read-only vlog files
  uint64_t vlog_file_number_{0};
  VLogCache* const vlog_cache_;
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_LOG_H
