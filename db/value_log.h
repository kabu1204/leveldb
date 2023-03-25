//
// Created by 于承业 on 2023/3/22.
//

#ifndef LEVELDB_VALUE_LOG_H
#define LEVELDB_VALUE_LOG_H

#include <vector>
#include <deque>

#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/env.h"
#include "table/format.h"
#include "port/port.h"

namespace leveldb {


struct VLogFileMeta {
  int refs{0};
  uint32_t number{0};
  uint64_t file_size{0};
};

class ValueLog {
 public:
  explicit ValueLog(const Options& options);

  ~ValueLog();

  ValueLog(const ValueLog&) = delete;
  ValueLog& operator=(const ValueLog&) = delete;

  virtual Status Add(const WriteOptions& options, const Slice& key, const Slice& value);

  virtual Status Get(const ReadOptions& options, const ValueHandle& handle, std::string* value);

 protected:
  port::Mutex mutex_;
  Options options_;
//  std::deque<VLogFileMeta*> files_;
//  std::atomic<bool> shutdown_{false};
};

class ValueLogSingle: public ValueLog {
  explicit ValueLogSingle(const Options& options);

  ~ValueLogSingle();

  ValueLogSingle(const ValueLogSingle&) = delete;
  ValueLogSingle& operator=(const ValueLogSingle&) = delete;

  Status Add(const WriteOptions& options, const Slice& key, const Slice& value);

  Status Get(const ReadOptions& options, const ValueHandle& handle, std::string* value);

 private:
  RandomAccessFile* file_;
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_LOG_H
