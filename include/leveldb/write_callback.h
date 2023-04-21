// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

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
