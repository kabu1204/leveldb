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
    if (value.size() > threshold) {
      vb->Put(sequence_, key, value);
      sequence_++;
    } else {
      small->Put(key, value);
    }
  }

  void Delete(const Slice& key) override { small->Delete(key); }

  SequenceNumber sequence_;
  size_t threshold;
  ValueBatch* vb;
  WriteBatch* small;
};

Status ValueLogImpl::Open(const Options& options, const std::string& dbname,
                          DBWrapper* db, ValueLogImpl** vlog) {
  assert(db != nullptr);
  if (vlog != nullptr) {
    *vlog = nullptr;
  }

  ValueLogImpl* vlog_impl = new ValueLogImpl(options, dbname, db);
  vlog_impl->Recover();
  *vlog = vlog_impl;

  return Status::OK();
}

DBWrapper::~DBWrapper() { delete vlog_; }

Status DBWrapper::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);  // mutex_::mu_ is locked
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // &w == writers_.front() is TRUE
  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatch small;
    ValueBatch large;
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    status = DivideWriteBatch(write_batch, &small, &large);
    if (!status.ok()) {
      return status;
    }

    last_sequence += WriteBatchInternal::Count(write_batch);

    write_batch->Clear();  // write_batch may be tmp_batch_, we need to clear it
                           // when holding the lock.
    {
      mutex_.Unlock();
      status = vlog_->Write(options, &large);

      /*
       * large values have been inserted to ValueLog, format the WriteBatch
       * to the LSMTree.
       * TODO: can be optimized to reduce memory copy;
       */
      if (large.NumEntries() == 0) {
        write_batch = &small;
      } else {
        status = large.ToWriteBatch(write_batch);
        write_batch->Append(small);
      }
      assert(WriteBatchInternal::Sequence(write_batch) + large.NumEntries() ==
             WriteBatchInternal::Sequence(&small));

      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    //    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {  // TODO: maybe there's no need to notify self?
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

SequenceNumber DBWrapper::GetSmallestSnapshot() {
  MutexLock l(&mutex_);
  if (snapshots_.empty()) {
    return versions_->LastSequence();
  } else {
    return snapshots_.oldest()->sequence_number();
  }
}

Status DBWrapper::DivideWriteBatch(WriteBatch* input, WriteBatch* small,
                                   ValueBatch* large) {
  assert(input != nullptr && small != nullptr && large != nullptr);
  DivideHandler handler;
  handler.threshold = options_.vlog_value_size_threshold;
  handler.small = small;
  handler.vb = large;
  handler.sequence_ = WriteBatchInternal::Sequence(input);
  Status s = input->Iterate(&handler);
  WriteBatchInternal::SetSequence(small, handler.sequence_);
  return s;
}

Status DBWrapper::WriteLSM(const WriteOptions& options, WriteBatch* batch) {
  Status status;
  status = log_->AddRecord(WriteBatchInternal::Contents(batch));
  bool sync_error = false;
  if (status.ok() && options.sync) {
    status = logfile_->Sync();
    if (!status.ok()) {
      sync_error = true;
    }
  }
  if (status.ok()) {
    status = WriteBatchInternal::InsertInto(batch, mem_);
  }
  if (sync_error) {
    // The state of the log file is indeterminate: the log record we
    // just added may or may not show up when the DB is re-opened.
    // So we force the DB into a mode where all future writes fail.
    MutexLock l(&mutex_);
    RecordBackgroundError(status);
  }
  return status;
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
 * incorrect), because the corresponding .vlog file maybe deleted or rewritten
 * between <R_PTR> and <R_VLOG>.
 */
Status DBWrapper::Get(const ReadOptions& options, const Slice& key,
                      std::string* value) {
  ValueType valueType;
  Status status = DBImpl::Get(options, key, value, &valueType);
  if (!status.ok() || valueType != kTypeValueHandle) {
    return status;
  }

  ValueHandle handle;
  Slice input(*value);
  handle.DecodeFrom(&input);
  status = vlog_->Get(options, handle, value);

  return status;
}

std::string DBWrapper::DebugString() {
  std::string result;
  DBImpl::GetProperty("leveldb.stats", &result);
  result += "\n";
  result += vlog_->DebugString();
  return result;
}

Iterator* DBWrapper::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewBlobDBIterator(
      this, vlog_, user_comparator(), iter,
      (options.snapshot != nullptr
           ? static_cast<const SnapshotImpl*>(options.snapshot)
                 ->sequence_number()
           : latest_snapshot),
      seed);
}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;
  DBImpl* impl;
  if (!options.blob_db) {
    impl = new DBImpl(options, dbname);
  } else {
    Log(options.info_log,
        "Creating BlobDB, ValueThreshold = %lu\n\t"
        "VLogFileSize = %lu\n\t"
        "VLogMaxEntries = %lu",
        options.vlog_value_size_threshold, options.max_vlog_file_size,
        options.max_entries_per_vlog);
    DBWrapper* dbWrapper = new DBWrapper(options, dbname);
    ValueLogImpl* vlog;
    ValueLogImpl::Open(options, dbname, dbWrapper, &vlog);
    dbWrapper->vlog_ = vlog;
    impl = dbWrapper;
  }
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();

  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

}  // namespace leveldb