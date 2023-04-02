//
// Created by 于承业 on 2023/3/22.
//

#ifndef LEVELDB_VALUE_LOG_IMPL_H
#define LEVELDB_VALUE_LOG_IMPL_H

#include "db/value_table_cache.h"
#include <deque>
#include <map>
#include <set>
#include <unordered_map>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/value_log.h"

#include "port/port.h"
#include "table/format.h"
#include "table/vlog.h"

namespace leveldb {

struct VLogFileMeta {
  VLogFileMeta() {}

  int refs{0};
  uint64_t number;
  uint64_t file_size{0};
};

// thread-safe
class ValueLogImpl : public ValueLog {
 public:
  ~ValueLogImpl() override;

  ValueLogImpl(const ValueLogImpl&) = delete;
  ValueLogImpl& operator=(const ValueLogImpl&) = delete;

  Status Add(const WriteOptions& options, const Slice& key, const Slice& value,
             ValueHandle* handle) override;

  Status Get(const ReadOptions& options, const ValueHandle& handle,
             std::string* value) override;

  Status Recover();

  std::string DebugString() override;

 private:
  friend class ValueLog;

  explicit ValueLogImpl(const Options& options, const std::string& dbname);

  uint64_t NewFileNumber() { return ++vlog_file_number_; }
  uint64_t CurrentFileNumber() const { return vlog_file_number_; }
  void ReuseFileNumber(uint64_t number) {
    if (number == CurrentFileNumber() + 1) {
      vlog_file_number_ = number;
    }
  }

  Status FinishBuild();
  Status NewVLogBuilder();

  struct ByFileNumber {
    bool operator()(VLogFileMeta* lhs, VLogFileMeta* rhs) const {
      return lhs->number < rhs->number;
    }
  };

  // TODO: use ref instead
  struct FileAndRef {
    std::atomic<int> refs{0};
    AppendableRandomAccessFile* file{nullptr};
  };

  //  port::Mutex mutex_;        // protect class members
  port::RWSpinLock rwlock_;  // protect class members
  Options options_;
  Env* const env_;
  const std::string dbname_;
  std::atomic<bool> shutdown_;

  AppendableRandomAccessFile* rw_file_;  // the file that currently being built
  VLogBuilder* builder_;                 // associated with rw_file_
  VLogReader* reader_ GUARDED_BY(rwlock_);  // associated with rw_file_

  std::map<uint64_t, VLogFileMeta*> ro_files_
      GUARDED_BY(rwlock_);  // read-only vlog files
  uint64_t vlog_file_number_{0};
  VLogCache* const vlog_cache_{nullptr};
  std::unordered_map<uint64_t, port::RWSpinLock*> lock_table_
      GUARDED_BY(rwlock_);
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_LOG_IMPL_H
