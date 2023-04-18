//
// Created by 于承业 on 2023/3/22.
//

#ifndef LEVELDB_VALUE_LOG_IMPL_H
#define LEVELDB_VALUE_LOG_IMPL_H

#include "db/db_impl.h"
#include "db/db_wrapper.h"
#include "db/log_writer.h"
#include "db/value_log_version.h"
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

class VLogRWFile {
 public:
  VLogRWFile(const Options& options, AppendableRandomAccessFile* f,
             uint64_t number, bool reuse = false, uint32_t offset = 0,
             uint32_t num_entries = 0)
      : file_(f),
        builder_(nullptr),
        reader_(nullptr),
        closed_(false),
        number_(number),
        refs_(0) {
    if (file_ != nullptr) {
      builder_ = new VLogBuilder(options, file_, reuse, offset, num_entries);
      status_ = VLogReader::Open(options, file_, offset, &reader_);
    }
  }

  VLogRWFile(const VLogRWFile&) = delete;
  VLogRWFile& operator=(const VLogRWFile&) = delete;

  /*
   * calling Ref() and Unref() concurrently needs external synchronization.
   * See comments in Unref() below.
   */
  void Ref() { ++refs_; }
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      /*
       * There are chances that another thread calls Ref(), and current thread
       * are deleting this. However, in ValueLogImpl, this never happens.
       *
       * ValueLogImpl will call Ref() when creating new VlogRWFile, so before it
       * Unref() (i.e. finish building and creating another one), the refs_ will
       * always be >= 1;
       *
       * Before ValueLogImpl Unref() this, concurrent Ref() and Unref() is safe,
       * because refs_<=0 will always be false, the rwfile can never be deleted
       * by mistake.
       *
       * After ValueLogImpl Unref() this, no more Ref() happens, so it's safe.
       */
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
  uint64_t FileNumber() const { return number_; }

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
  uint64_t number_;

  AppendableRandomAccessFile* const
      file_;              // the file that currently being built
  VLogBuilder* builder_;  // associated with rw_file_
  VLogReader* reader_;    // associated with rw_file_
};

class ValueLogGCWriteCallback;
struct GarbageCollection;

/*
 * thread-safe for 1 Add() and N Get()
 */
class ValueLogImpl {
 public:
  static Status Open(const Options& options, const std::string& dbname, DB* db,
                     ValueLogImpl** vlog);

  virtual ~ValueLogImpl();

  ValueLogImpl(const ValueLogImpl&) = delete;
  ValueLogImpl& operator=(const ValueLogImpl&) = delete;

  /*
   * concurrent Put() is unsafe
   */
  Status Put(const WriteOptions& options, const Slice& key, const Slice& value,
             ValueHandle* handle);

  Status Write(const WriteOptions& options, ValueBatch* batch);

  Status Get(const ReadOptions& options, const ValueHandle& handle,
             std::string* value);

  //  Status MultiGet(const ReadOptions& options, std::vector<ValueHandle>&
  //  handles,
  //                  std::vector<std::string>* values);

  Status Recover();

  std::string DebugString();

  GarbageCollection* PickGC(uint64_t number);

  Status Collect(GarbageCollection* gc) SHARED_LOCKS_REQUIRED(rwlock_);

  Status Rewrite(GarbageCollection* gc);

  Status ManualGC(uint64_t number);

  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(rwlock_);

 private:
  friend class DBWrapper;

  Status GetLocked(const ReadOptions& options, VLogRWFile* rwfile,
                   const ValueHandle& handle, std::string* value)
      SHARED_LOCKS_REQUIRED(rwlock_);

  explicit ValueLogImpl(const Options& options, const std::string& dbname,
                        DB* db);

  uint64_t NewFileNumber() EXCLUSIVE_LOCKS_REQUIRED(rwlock_) {
    return ++vlog_file_number_;
  }

  void MarkFileNumberUsed(uint64_t number) EXCLUSIVE_LOCKS_REQUIRED(rwlock_) {
    if (number > CurrentFileNumber()) {
      vlog_file_number_ = number;
    }
  }

  uint64_t CurrentFileNumber() const { return vlog_file_number_; }

  Status FinishBuild() EXCLUSIVE_LOCKS_REQUIRED(rwlock_);

  Status NewVLogBuilder() EXCLUSIVE_LOCKS_REQUIRED(rwlock_);

  Iterator* NewVLogFileIterator(const ReadOptions& options, uint64_t number);

  Status WriteSnapshot(log::Writer* log) EXCLUSIVE_LOCKS_REQUIRED(rwlock_);

  Status LogAndApply(BlobVersionEdit* edit) EXCLUSIVE_LOCKS_REQUIRED(rwlock_);

  Status ValidateAndTruncate(uint64_t number, uint64_t* offset,
                             uint64_t* num_entries, uint64_t* file_size);

  Status CheckBlobDBExistence();

  Status NewBlobDB();

  struct ByFileNumber {
    bool operator()(VLogFileMeta* lhs, VLogFileMeta* rhs) const {
      return lhs->number < rhs->number;
    }
  };

  port::RWSpinLock rwlock_;  // protect class members
  const std::string dbname_;
  Options options_;
  Env* const env_;
  DBImpl* const db_;              // LSM
  uint64_t vlog_file_number_{0};  // current maximum file number
  uint64_t manifest_number_{0};
  std::atomic<bool> shutdown_;

  /*
   * MANIFEST
   */
  WritableFile* manifest_file_;
  log::Writer* manifest_log_;

  /*
   * Read & Write VLOG
   */
  VLogRWFile* rwfile_;
  std::map<uint64_t, VLogFileMeta> ro_files_
      GUARDED_BY(rwlock_);  // read-only vlog files

  VLogCache* const vlog_cache_{nullptr};

  /*
   * Garbage collection
   */
  std::set<uint64_t> pending_outputs_;  // GC thread is writing to these files
  std::map<uint64_t, SequenceNumber>
      obsolete_files_;  // <file_number, sequence>
};

}  // namespace leveldb

#endif  // LEVELDB_VALUE_LOG_IMPL_H
