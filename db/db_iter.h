// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_ITER_H_
#define STORAGE_LEVELDB_DB_DB_ITER_H_

#include <cstdint>

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace leveldb {

class DBImpl;
class ValueLogImpl;

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator,
                        Iterator* internal_iter, SequenceNumber sequence,
                        uint32_t seed);

Iterator* NewBlobDBIterator(DBImpl* db, ValueLogImpl* vlog, Env* env,
                            const Comparator* user_key_comparator,
                            Iterator* internal_iter, SequenceNumber sequence,
                            uint32_t seed, bool prefetch, int max_prefetch);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_ITER_H_
