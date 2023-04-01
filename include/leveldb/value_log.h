//
// Created by 于承业 on 2023/4/2.
//

#ifndef LEVELDB_VALUE_LOG_H
#define LEVELDB_VALUE_LOG_H

#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {

class ValueHandle;

class LEVELDB_EXPORT ValueLog {
 public:
  static Status Open(const Options& options, const std::string& dbname,
                     ValueLog** vlog);

  ValueLog() = default;

  virtual ~ValueLog();

  ValueLog(const ValueLog&) = delete;
  ValueLog& operator=(const ValueLog&) = delete;

  virtual Status Add(const WriteOptions& options, const Slice& key,
                     const Slice& value, ValueHandle* handle) = 0;

  virtual Status Get(const ReadOptions& options, const ValueHandle& handle,
                     std::string* value) = 0;

  virtual std::string DebugString() = 0;
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_LOG_H
