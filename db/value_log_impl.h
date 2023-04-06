//
// Created by 于承业 on 2023/3/22.
//

#ifndef LEVELDB_VALUE_LOG_IMPL_H
#define LEVELDB_VALUE_LOG_IMPL_H

#include "db/db_impl.h"
#include "db/db_wrapper.h"
#include "db/log_writer.h"
#include "db/value_table_cache.h"
#include <deque>
#include <map>
#include <set>
#include <unordered_map>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

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

class VLogRWFile {
 public:
  VLogRWFile(const Options& options, AppendableRandomAccessFile* f,
             bool reuse = false, uint32_t offset = 0, uint32_t num_entries = 0)
      : file_(f),
        builder_(nullptr),
        reader_(nullptr),
        closed_(false),
        refs_(0) {
    if (file_ != nullptr) {
      builder_ = new VLogBuilder(options, file_, reuse, offset, num_entries);
      status_ = VLogReader::Open(options, file_, offset, &reader_);
    }
  }

  VLogRWFile(const VLogRWFile&) = delete;
  VLogRWFile& operator=(const VLogRWFile&) = delete;

  /*
   * Ref()/Unref() are not thread-safe and need external synchronization.
   */
  void Ref() { ++refs_; }
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  void Write(const ValueBatch* batch) {
    assert(!closed_);
    builder_->AddBatch(batch);
    reader_->IncreaseOffset(builder_->Offset());
  }

  // should be called after Ref()
  void Add(const Slice& key, const Slice& val, ValueHandle* handle) {
    assert(!closed_);
    builder_->Add(key, val, handle);
    reader_->IncreaseOffset(builder_->Offset());
  }

  void Flush() {
    assert(!closed_);
    builder_->Flush();
  }

  void Sync() {
    assert(!closed_);
    file_->Sync();
  }

  void Finish() {
    assert(!closed_);
    closed_ = true;
    file_->Sync();
    builder_->Finish();
    file_->Close();
  }

  Iterator* NewIterator(const ReadOptions& options) {
    Ref();
    Iterator* iter = reader_->NewIterator(options);
    iter->RegisterCleanup(&UnrefCleanup, this, nullptr);
    return iter;
  }

  uint32_t Offset() const { return builder_->Offset(); }
  uint32_t FileSize() const { return builder_->FileSize(); }

 private:
  ~VLogRWFile() {
    assert(closed_);
    delete reader_;
    delete builder_;
    delete file_;
  }

  static void UnrefCleanup(void* arg1, void* arg2) {
    VLogRWFile* p = reinterpret_cast<VLogRWFile*>(arg1);
    p->Unref();
  }

  std::atomic<int> refs_;
  bool closed_;

  Status status_;

  AppendableRandomAccessFile* const
      file_;              // the file that currently being built
  VLogBuilder* builder_;  // associated with rw_file_
  VLogReader* reader_;    // associated with rw_file_
};

/*
 * thread-safe for 1 Add() and N Get()
 */
class ValueLogImpl {
 public:
  static Status Open(const Options& options, const std::string& dbname,
                     DBWrapper* db, ValueLogImpl** vlog);

  virtual ~ValueLogImpl();

  ValueLogImpl(const ValueLogImpl&) = delete;
  ValueLogImpl& operator=(const ValueLogImpl&) = delete;

  /*
   * concurrent Add() is unsafe
   */
  Status Put(const WriteOptions& options, const Slice& key, const Slice& value,
             uint64_t seq, ValueHandle* handle);

  Status Write(const WriteOptions& options, ValueBatch* batch);

  Status Get(const ReadOptions& options, const ValueHandle& handle,
             std::string* value);

  Status Recover();

  std::string DebugString();

  void BackgroundGC();

  static void BGWork(void* vlog);

 private:
  friend class ValueLog;
  friend Status ValueLogImpl::Open(const Options& options,
                                   const std::string& dbname, DBWrapper* db,
                                   ValueLogImpl** vlog);

  explicit ValueLogImpl(const Options& options, const std::string& dbname,
                        DBImpl* db);

  uint64_t NewFileNumber() { return ++vlog_file_number_; }

  uint64_t CurrentFileNumber() const { return vlog_file_number_; }
  void ReuseFileNumber(uint64_t number) {
    if (number == CurrentFileNumber() + 1) {
      vlog_file_number_ = number;
    }
  }

  Status FinishBuild();

  Status NewVLogBuilder();

  Iterator* NewVLogFileIterator(const ReadOptions& options, uint64_t number);

  uint64_t PickGC();

  struct ByFileNumber {
    bool operator()(VLogFileMeta* lhs, VLogFileMeta* rhs) const {
      return lhs->number < rhs->number;
    }
  };

  port::RWSpinLock rwlock_;  // protect class members
  const std::string dbname_;
  Options options_;
  Env* const env_;
  DB* const db_;

  /*
   * Read & Write VLOG
   */
  VLogRWFile* rwfile_;
  std::map<uint64_t, VLogFileMeta*> ro_files_
      GUARDED_BY(rwlock_);  // read-only vlog files
  uint64_t vlog_file_number_{0};
  VLogCache* const vlog_cache_{nullptr};
  std::unordered_map<uint64_t, port::RWSpinLock*> lock_table_
      GUARDED_BY(rwlock_);
  char buf_[256];

  /*
   * Garbage collection
   */
  uint64_t gc_ptr_;
  WritableFile* hintfile_;
  log::Writer* hint_;
  std::atomic<bool> shutdown_;
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_LOG_IMPL_H
