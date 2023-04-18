//
// Created by 于承业 on 2023/4/11.
//

#include "db/db_wrapper.h"
#include "db/filename.h"
#include "db/value_log_impl.h"
#include "db/value_log_version.h"
#include "db/write_batch_internal.h"

#include "leveldb/status.h"

#include "util/sync_point.h"

namespace leveldb {

class ValueLogGCWriteCallback : public WriteCallback {
 public:
  ~ValueLogGCWriteCallback() override = default;
  ValueLogGCWriteCallback(std::string&& key, ValueHandle handle)
      : key_(key), handle_(handle) {}

  Status Callback(DB* db) override {
    Status s;
    std::string value;
    s = db->Get(ReadOptions(), key_, &value);
    if (!s.ok()) {
      return s;
    }

    Slice input(value);
    ValueHandle current;
    current.DecodeFrom(&input);
    if (current != handle_) {
      return Status::InvalidArgument("KVHandle may be overwritten");
    }

    return Status::OK();
  }

  bool AllowGrouping() const override { return false; }

  Slice key() const { return {key_}; }

 private:
  friend class ValueLogImpl;

  std::string key_;
  ValueHandle handle_;
};

/*
 * TODO: collect several files in one GC
 */
struct GarbageCollection {
  GarbageCollection()
      : number(0),
        obsolete_sequence(0),
        total_size(0),
        total_entries(0),
        discard_size(0),
        discard_entries(0) {}
  ~GarbageCollection() = default;
  uint64_t number;
  ValueBatch value_batch;
  std::vector<std::pair<WriteBatch, ValueLogGCWriteCallback>> rewrites;
  uint32_t total_size;
  uint32_t total_entries;
  uint32_t discard_size;
  uint32_t discard_entries;
  SequenceNumber obsolete_sequence;
  Status s;
};

/*
 * We not have any policies :)
 * We just pick the first valid vlog whose file_number >= number.
 */
GarbageCollection* ValueLogImpl::PickGC(uint64_t number) {
  rwlock_.AssertRLockHeld();

  while (true) {
    auto it = ro_files_.lower_bound(number);
    if (it == ro_files_.end()) {
      Log(options_.info_log, "PickGC Restart");
      return nullptr;
    }
    if (obsolete_files_.find(it->first) == obsolete_files_.end()) {
      number = it->first;
      break;
    }
    number = it->first + 1;
  }

  GarbageCollection* gc = new GarbageCollection();
  gc->number = number;
  return gc;
}

Status ValueLogImpl::ManualGC(uint64_t number) {
  ReadLock l(&rwlock_);
  Status s;
  GarbageCollection* gc = PickGC(number);
  if (gc != nullptr) {
    s = Collect(gc);
    if (!s.ok()) {
      return s;
    }
    rwlock_.RUnlock();
    s = Rewrite(gc);
    rwlock_.RLock();
  } else {
    return Status::InvalidArgument("Do not find a valid vlog for GC");
  }
  return s;
}

Status ValueLogImpl::Collect(GarbageCollection* gc) {
  rwlock_.AssertRLockHeld();
  assert(gc != nullptr);
  Log(options_.info_log, "Collecting old entries in vlog %llu\n", gc->number);
  Status s;
  uint64_t number = gc->number;
  if (number >= CurrentFileNumber() || number == 0) {
    return Status::InvalidArgument("invalid file number",
                                   std::to_string(number));
  }

  VLogReaderIterator* iter = reinterpret_cast<VLogReaderIterator*>(
      NewVLogFileIterator(ReadOptions(), number));
  if (iter == nullptr) {
    return Status::InvalidArgument("invalid file number",
                                   std::to_string(number));
  }

  rwlock_.RUnlock();
  uint64_t seq;
  Slice ikey, key;
  std::string handle_encoding;
  ValueHandle handle, current;
  ValueBatch& vb = gc->value_batch;
  std::vector<std::pair<WriteBatch, ValueLogGCWriteCallback>>& rewrites =
      gc->rewrites;
  current.table_ = number;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ikey = iter->key();
    if (ValueBatch::GetInternalKeySeq(&ikey, ikey.size(), &key, &seq)) {
      s = db_->Get(ReadOptions(), key, &handle_encoding);
      if (!s.ok() && !s.IsNotFound()) {
        s = Status::IOError("[GC] failed to Get from DBImpl", s.ToString());
        break;
      }

      iter->GetValueHandle(&current);

      gc->total_entries++;
      gc->total_size += current.size_;

      Slice input(handle_encoding);
      handle.DecodeFrom(&input);
      if (s.IsNotFound() || handle != current) {
        gc->discard_entries++;
        gc->discard_size += current.size_;
        continue;
      }

      /*
       * We need to keep the entry.
       */
      vb.Put(seq, key, iter->value());
      rewrites.emplace_back(WriteBatch(),
                            ValueLogGCWriteCallback(key.ToString(), handle));
    } else {
      s = Status::Corruption("[GC] failed to decode vlog internal key", ikey);
      break;
    }
  }
  rwlock_.RLock();
  delete iter;
  return s;
}

/*
 * Rewrite will write to ValueLog, it needs external synchronization.
 *
 * Crash consistency:
 *  1. Crash happens before LSM rewrite:
 *      This leaves an untracked .vlog file, it will be marked deleted by GC
 * later.
 *  2. Crash happens before apply BlobVersionEdit:
 *       This also leaves an untracked .vlog file, but the LSM contains
 * ValueHandles pointing to this file. When recovering, we validate and add the
 * untracked file to ro_files.
 */
Status ValueLogImpl::Rewrite(GarbageCollection* gc) {
  Status s;
  assert(gc != nullptr);
  Log(options_.info_log, "Rewriting vlog %llu\n", gc->number);
  Log(options_.info_log, "[GC #%llu] Size based discard ratio: %u/%u = %d%%",
      gc->number, gc->discard_size, gc->total_size,
      (gc->discard_size * 100 / gc->total_size));
  Log(options_.info_log, "[GC #%llu] Num based discard ratio: %u/%u = %d%%",
      gc->number, gc->discard_entries, gc->total_entries,
      (gc->discard_entries * 100 / gc->total_entries));
  if ((gc->discard_size * 100 / gc->total_size) <
          options_.blob_gc_size_discard_threshold &&
      (gc->discard_entries * 100 / gc->total_entries) <
          options_.blob_gc_num_discard_threshold) {
    return Status::InvalidArgument(
        "Discarded entries/size does not reach the threshold");
  }

  if (gc->discard_entries == gc->total_entries) {
    Log(options_.info_log,
        "[GC #%llu] All entries discarded, removing the entire file",
        gc->number);
    gc->obsolete_sequence = db_->LatestSequence();
    BlobVersionEdit edit;
    edit.DeleteFile(gc->number, gc->obsolete_sequence);
    WriteLock l(&rwlock_);
    return LogAndApply(&edit);
  }

  /*
   * we create another file instead of directly writing to rwfile_
   */
  AppendableRandomAccessFile* file;
  VLogBuilder* builder;
  uint64_t number;
  {
    WriteLock l(&rwlock_);
    number = NewFileNumber();
    s = env_->NewAppendableRandomAccessFile(VLogFileName(dbname_, number),
                                            &file);
    if (!s.ok()) {
      return s;
    }
    builder = new VLogBuilder(options_, file, false);
    pending_outputs_.emplace(number);

    Log(options_.info_log, "[GC #%llu] Rewriting to vlog#%llu", gc->number,
        number);
  }

  /*
   * The valid values that we are rewriting to the new vlog file, are already
   * persistent. So we need to keep the consistency.
   */
  WriteOptions opt;
  opt.sync = true;

  // 1. write to ValueLog
  gc->value_batch.Finalize(number, 0);
  builder->AddBatch(&gc->value_batch);
  file->Sync();
  builder->Finish();
  file->Close();

  // add file to ro_files in advance
  VLogFileMeta f;
  f.number = number;
  f.file_size = builder->FileSize();
  {
    rwlock_.WLock();
    ro_files_.emplace(number, f);
    rwlock_.WUnlock();
  }
  delete builder;
  delete file;

  TEST_SYNC_POINT_MAY_RETURN("GC.Rewrite.AfterValueRewrite", s);

  // 2. rewrite to LSM
  // we disable sync when writing, and manually sync LSM later
  Log(options_.info_log, "[GC #%llu] Rewriting to LSM, vlog#%llu", gc->number,
      number);
  opt.sync = false;
  int i = 0;
  std::string handle_encoding;
  const std::vector<ValueHandle>& handles = gc->value_batch.Handles();
  for (auto& p : gc->rewrites) {  // <WriteBatch, ValueLogGCWriteCallback>
    // set rewrite value handles
    handles[i++].EncodeTo(&handle_encoding);
    WriteBatchInternal::Put(&p.first, p.second.key(), handle_encoding,
                            kTypeValueHandle);

    // write to LSM
    s = db_->Write(opt, &p.first, &p.second);
    if (!s.ok()) {
      return Status::IOError("failed to write to LSM", s.ToString());
    }
  }
  s = db_->Sync();
  if (!s.ok()) {
    return s;
  }

  TEST_SYNC_POINT_MAY_RETURN("GC.Rewrite.AfterLSMRewrite", s);

  // 3. mark old file as obsolete
  gc->obsolete_sequence = db_->LatestSequence();
  BlobVersionEdit edit;
  edit.AddFile(f.number, f.file_size);
  edit.DeleteFile(gc->number, gc->obsolete_sequence);
  rwlock_.WLock();
  s = LogAndApply(&edit);  // we mark the file as obsolete here, it will be
                           // removed from disk at proper time.
  rwlock_.WUnlock();
  return s;
}

}  // namespace leveldb