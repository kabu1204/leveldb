//
// Created by 于承业 on 2023/4/4.
//

#include "db/db_wrapper.h"

#include "db/db_iter.h"
#include "db/filename.h"
#include "db/memtable.h"
#include "db/value_log_impl.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"

#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {

class DivideHandler : public WriteBatch::Handler {
 public:
  DivideHandler() = default;
  ~DivideHandler() override = default;

  void Put(const Slice& key, const Slice& value) override {
    if (value.size() >= threshold) {
      vb->Put(key, value);
    } else {
      small->Put(key, value);
    }
  }

  void Delete(const Slice& key) override { small->Delete(key); }

  size_t threshold;
  ValueBatch* vb;
  WriteBatch* small;
};

// Information kept for every waiting writer
struct DBWrapper::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

Status ValueLogImpl::Open(const Options& options, const std::string& dbname,
                          DB* db, ValueLogImpl** vlog) {
  assert(db != nullptr);
  if (vlog != nullptr) {
    *vlog = nullptr;
  }

  ValueLogImpl* vlog_impl = new ValueLogImpl(options, dbname, db);
  vlog_impl->Recover();
  *vlog = vlog_impl;

  return Status::OK();
}

DBWrapper::~DBWrapper() {
  delete vlog_;
  delete db_;
}

Status DBWrapper::Write(const WriteOptions& options, WriteBatch* updates) {
  return Write(options, updates, nullptr);
}

Status DBWrapper::Write(const WriteOptions& options, WriteBatch* updates,
                        WriteCallback* callback) {
  // TODO batching the updates;
  Status s;
  WriteBatch small;
  ValueBatch large;
  s = DivideWriteBatch(updates, &small, &large);
  if (!s.ok()) {
    return s;
  }

  {
    WriteLock l(&rwlock_);
    s = vlog_->Write(options, &large);
  }
  if (s.ok()) {
    large.ToWriteBatch(&small);
    s = db_->Write(options, &small, callback);
  }
  return s;
}

Status DBWrapper::DivideWriteBatch(WriteBatch* input, WriteBatch* small,
                                   ValueBatch* large) {
  assert(input != nullptr && small != nullptr && large != nullptr);
  DivideHandler handler;
  handler.threshold = options_.blob_value_size_threshold;
  handler.small = small;
  handler.vb = large;
  Status s = input->Iterate(&handler);
  return s;
}

/*
 * thread safety analysis:
 * <W_VLOG> Write to VLOG   <W_PTR> Write pointer to LSM
 * <R_VLOG> Read from VLOG  <R_PTR> Read value pointer from LSM
 *
 * Both VLOG and LSM are internally thread-safe themselves (one Writer and
 * multiple Readers).
 *
 * Assuming GC is disabled, <R_VLOG> always return the correct value of the
 * ValueHandle retrieved by <R_PTR>, because we <W_VLOG> before <W_PTR>.
 *
 * When GC is enabled, <R_VLOG> may return the bad value(either NotFound or
 * incorrect), because the corresponding .vlog file maybe deleted between
 * <R_PTR> and <R_VLOG>. We address this issue without extra synchronization,
 * see comments below.
 */
Status DBWrapper::Get(const ReadOptions& options, const Slice& key,
                      std::string* value) {
  ValueType valueType;
  Status s = db_->Get(options, key, value, &valueType);
  if (!s.ok() || valueType != kTypeValueHandle) {
    return s;
  }

  ValueHandle handle;
  Slice input(*value);
  handle.DecodeFrom(&input);
  s = vlog_->Get(options, handle, value);

  if (s.ok() || options.snapshot != nullptr || !s.IsNotFound()) {
    // most cases
  } else if (!s.IsNotFound()) {
    // maybe a fatal error
  } else if (options.snapshot != nullptr) {
    /*
     * vlog->Get() returns a not found, indicating the target file is not found.
     *
     * When options.snapshot != nullptr, the vlog guarantees the file pointed by
     * handle will not be removed.
     *
     * When options.snapshot == nullptr, the target file maybe removed by vlog
     * because the smallest sequence of DB > the obsolete sequence of the file.
     * (see comments of ValueLogImpl::Collect and
     * ValueLogImpl::RemoveObsoleteFiles)
     *
     * In the later case (which is rare), we GetSnapshot and retry.
     */
  } else {
    ReadOptions opt = options;
    opt.snapshot = db_->GetSnapshot();
    s = Get(opt, key, value);
    db_->ReleaseSnapshot(opt.snapshot);
    return s;
  }
  return s;
}

Status DBWrapper::Put(const WriteOptions& options, const Slice& key,
                      const Slice& val) {
  return DB::Put(options, key, val);
}

Status DBWrapper::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

std::string DBWrapper::DebugString() {
  std::string result;
  db_->GetProperty("leveldb.stats", &result);
  result += "\n";
  result += vlog_->DebugString();
  return result;
}

static void ReleaseBlobDBIterSnapshot(void* arg1, void* arg2) {
  reinterpret_cast<DBWrapper*>(arg1)->ReleaseSnapshot(
      reinterpret_cast<const Snapshot*>(arg2));
}

Iterator* DBWrapper::NewIterator(const ReadOptions& options) {
  const Snapshot* snapshot = options.snapshot;
  if (!snapshot) {
    // If user doesn't specify snapshot, we have to get a new snapshot.
    // See comments of DBWrapper::Get.
    snapshot = GetSnapshot();
  }
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = db_->NewInternalIterator(options, &latest_snapshot, &seed);
  Iterator* blob_iter = NewBlobDBIterator(
      db_, vlog_, options_.env, db_->user_comparator(), iter,
      static_cast<const SnapshotImpl*>(snapshot)->sequence_number(), seed,
      options.blob_prefetch, options_.blob_background_read_threads);

  if (!options.snapshot) {
    // If user doesn't specify snapshot, we need to release the snapshot we got
    // above after the iter is deleted.
    blob_iter->RegisterCleanup(&ReleaseBlobDBIterSnapshot, this,
                               (void*)(snapshot));
  }

  return blob_iter;
}

const Snapshot* DBWrapper::GetSnapshot() { return db_->GetSnapshot(); }

void DBWrapper::ReleaseSnapshot(const Snapshot* snapshot) {
  db_->ReleaseSnapshot(snapshot);
}

bool DBWrapper::GetProperty(const Slice& property, std::string* value) {
  return db_->GetProperty(property, value);
}

void DBWrapper::GetApproximateSizes(const Range* range, int n,
                                    uint64_t* sizes) {
  db_->GetApproximateSizes(range, n, sizes);
}

void DBWrapper::CompactRange(const Slice* begin, const Slice* end) {
  db_->CompactRange(begin, end);
}

Status DBWrapper::Open(const Options& options, const std::string& name,
                       DBWrapper** dbptr) {
  if (dbptr != nullptr) {
    *dbptr = nullptr;
  }
  Log(options.info_log,
      "Creating BlobDB, "
      "ValueThreshold = %lu\n\t"
      "BlobMaxFileSize = %lu\n\t"
      "BlobGCSizeDiscardThreshold = %d\n\t"
      "BlobGCNumDiscardThreshold = %d\n\t"
      "BlobGCInterval = %d\n\t"
      "BlobBackgroundReadThreads = %d",
      options.blob_value_size_threshold, options.blob_max_file_size,
      options.blob_gc_size_discard_threshold,
      options.blob_gc_num_discard_threshold, options.blob_gc_interval,
      options.blob_background_read_threads);
  Status s;
  DB* db;
  s = DB::Open(options, name, &db);
  if (!s.ok()) {
    return s;
  }
  ValueLogImpl* vlog;
  s = ValueLogImpl::Open(options, name, db, &vlog);
  if (!s.ok()) {
    delete db;
    return s;
  }
  options.env->SetPoolBackgroundThreads(
      options.blob_background_read_threads + 1 +
      options.env->GetPoolBackgroundThreads());
  DBWrapper* dbWrapper = new DBWrapper(options, name, db, vlog);
  if (dbptr != nullptr) {
    *dbptr = dbWrapper;
  }
  return s;
}

void DBWrapper::ManualGC(uint64_t number) { vlog_->ManualGC(number); }

Status DBWrapper::VLogBGError() {
  MutexLock l(&vlog_->mutex_);
  return vlog_->bg_error_;
}

Status DBWrapper::SyncLSM() { return db_->Sync(); }

void DBWrapper::RemoveObsoleteBlob() {
  WriteLock l(&vlog_->rwlock_);
  vlog_->RemoveObsoleteFiles();
}

void DBWrapper::WaitVLogGC() {
  MutexLock l(&vlog_->mutex_);
  while (vlog_->bg_garbage_collection_) {
    vlog_->bg_work_cv_.Wait();
  }
}

}  // namespace leveldb