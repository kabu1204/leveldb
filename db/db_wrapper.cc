//
// Created by 于承业 on 2023/4/4.
//

#include "db/db_wrapper.h"

#include "db/value_log_impl.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"

#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {

class DivideHandler : public WriteBatch::Handler {
 public:
  ~DivideHandler() override;

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
    {
      mutex_.Unlock();
      status = vlog_->Write(options, &large);

      /*
       * large values have been inserted to ValueLog, format the WriteBatch
       * to the LSMTree.
       * TODO: can be optimized to reduce memory copy;
       */
      write_batch->Clear();
      large.ToWriteBatch(write_batch);
      write_batch->Append(small);
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
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

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

}  // namespace leveldb