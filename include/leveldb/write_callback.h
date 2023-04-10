//
// Created by 于承业 on 2023/4/10.
//

#ifndef LEVELDB_WRITE_CALLBACK_H
#define LEVELDB_WRITE_CALLBACK_H

#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

class DB;

class LEVELDB_EXPORT WriteCallback {
 public:
  virtual ~WriteCallback(){};

  virtual Status Callback(DB* db) = 0;

  virtual bool AllowGrouping() const { return false; }
};

}  // namespace leveldb

#endif  // LEVELDB_WRITE_CALLBACK_H
