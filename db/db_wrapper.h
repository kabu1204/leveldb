//
// Created by 于承业 on 2023/4/4.
//

#ifndef LEVELDB_DB_WRAPPER_H
#define LEVELDB_DB_WRAPPER_H

#include "db/db_impl.h"
#include <vector>

#include "leveldb/write_batch.h"

#include "table/format.h"
#include "util/mutexlock.h"

namespace leveldb {

class ValueLogImpl;
class ValueBatch;

class DBWrapper : public DBImpl {
 public:
  DBWrapper(const DBWrapper&) = delete;
  DBWrapper& operator=(const DBWrapper&) = delete;

  ~DBWrapper() override;

  // Apply the specified updates to the database.
  // Returns OK on success, non-OK on failure.
  // Note: consider setting options.sync = true.
  Status Write(const WriteOptions& options, WriteBatch* updates) override;

  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;

  std::string DebugString();

 private:
  friend class DB;
  friend class ValueLogImpl;

  DBWrapper(const Options& options, const std::string& dbname)
      : DBImpl(options, dbname), vlog_(nullptr) {}

  Status WriteLSM(const WriteOptions& options, WriteBatch*);

  Status DivideWriteBatch(WriteBatch* input, WriteBatch* small,
                          ValueBatch* large);

  SequenceNumber GetSmallestSnapshot();

  ValueLogImpl* vlog_;
};

}  // namespace leveldb

#endif  // LEVELDB_DB_WRAPPER_H
