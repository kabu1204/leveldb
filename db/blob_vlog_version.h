// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#ifndef LEVELDB_BLOB_VLOG_VERSION_H
#define LEVELDB_BLOB_VLOG_VERSION_H

#include "db/dbformat.h"
#include <map>
#include <set>
#include <vector>

#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

struct VLogFileMeta {
  VLogFileMeta() : refs(0), number(0), file_size(0) {}

  int refs;
  uint64_t number;
  uint64_t file_size;
};

class ValueLogImpl;

class BlobVersionEdit {
 public:
  BlobVersionEdit()
      : has_next_file_number_(false),
        has_new_rw_file_(false),
        next_file_number_(0),
        new_rw_file_(0) {}

  ~BlobVersionEdit() = default;

  void AddFile(uint64_t file, uint64_t file_size) {
    VLogFileMeta f;
    f.number = file;
    f.file_size = file_size;
    new_files_.emplace_back(std::move(f));
  }

  void DeleteFile(uint64_t file, SequenceNumber s) {
    deleted_files_.emplace(file, s);
  }

  void SetNextFile(uint64_t number) {
    has_next_file_number_ = true;
    next_file_number_ = number;
  }

  void NewRWFile(uint64_t number) {
    has_new_rw_file_ = true;
    new_rw_file_ = number;
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

 private:
  friend class ValueLogImpl;

  bool has_next_file_number_;
  uint64_t next_file_number_;

  bool has_new_rw_file_;
  uint64_t new_rw_file_;

  std::map<uint64_t, SequenceNumber> deleted_files_;
  std::vector<VLogFileMeta> new_files_;
};

}  // namespace leveldb

#endif  // LEVELDB_BLOB_VLOG_VERSION_H
