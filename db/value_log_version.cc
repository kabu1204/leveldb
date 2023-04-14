//
// Created by 于承业 on 2023/4/13.
//

#include "db/value_log_version.h"

#include "leveldb/status.h"

#include "util/coding.h"

namespace leveldb {

enum Tag {
  kNextFileNumber = 0,
  kNewRWFile = 1,
  kDeletedFile = 2,
  kNewFile = 3
};

void BlobVersionEdit::EncodeTo(std::string* dst) const {
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }

  if (has_new_rw_file_) {
    PutVarint32(dst, kNewRWFile);
    PutVarint64(dst, new_rw_file_);
  }

  for (const auto& file : deleted_files_) {
    PutVarint32(dst, kDeletedFile);
    PutVarint64(dst, file.first);   // file number
    PutVarint64(dst, file.second);  // sequence
  }

  for (const auto& f : new_files_) {
    PutVarint32(dst, kNewFile);
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
  }
}

Status BlobVersionEdit::DecodeFrom(const Slice& src) {
  Status s;
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag;
  uint64_t number;
  VLogFileMeta f;
  f.refs = 0;

  while (msg == nullptr && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;
      case kNewRWFile:
        if (GetVarint64(&input, &new_rw_file_)) {
          has_new_rw_file_ = true;
        } else {
          msg = "new rw file";
        }
        break;
      case kDeletedFile:
        SequenceNumber seq;
        if (GetVarint64(&input, &number) && GetVarint64(&input, &seq)) {
          deleted_files_.emplace(number, seq);
        } else {
          msg = "deleted file";
        }
        break;
      case kNewFile:
        if (GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size)) {
          new_files_.push_back(f);
        } else {
          msg = "new file";
        }
        break;
      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != nullptr) {
    result = Status::Corruption("BlobVersionEdit", msg);
  }
  return result;
}

}  // namespace leveldb