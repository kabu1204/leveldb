//
// Created by 于承业 on 2023/3/22.
//

#include "db/value_log_impl.h"
#include "db/write_batch_internal.h"
#include "db/filename.h"

#include "port/port.h"
#include "util/mutexlock.h"

namespace leveldb {

Status WriteBatchInternal::InsertInto(const WriteBatch* batch, ValueLogImpl* vlog){
//  WriteBatch*
  return Status::OK();
}

static size_t EncodeInternalKey(const Slice& key, uint64_t seq, char* ptr) {
  std::memcpy(ptr, key.data(), key.size());
  EncodeFixed64(ptr+key.size(), seq);
  return key.size() + sizeof(uint64_t);
}

static size_t DecodeInternalKey(const char* ptr, const char* end, uint64_t* seq) {
  *seq = DecodeFixed64(end - sizeof(uint64_t));
  return end - ptr - sizeof(uint64_t);
}

static inline size_t SizeOfInternalKey(const Slice& key) {
  return key.size() + 8;
}

ValueLogImpl::ValueLogImpl(const Options& options, const std::string& dbname, DBImpl* db)
    : env_(options.env),
      db_(db),
      options_(options),
      dbname_(dbname),
      vlog_cache_(new VLogCache(dbname_, options, options.max_open_vlogs)),
      shutdown_(false),
      rwfile_(nullptr),
      vlog_file_number_(0) {}

Status ValueLogImpl::Put(const WriteOptions& options, const Slice& key,
                         const Slice& value, uint64_t seq, ValueHandle* handle) {
  Status s;
  ValueBatch batch;
  batch.Put(seq, key, value);
  s = Write(options, &batch);
  *handle = batch.handles_[0];
  return s;
}

Status ValueLogImpl::Write(const WriteOptions& options, ValueBatch* batch) {
  Status s;
  batch->Finalize(CurrentFileNumber(), rwfile_->Offset());
  rwfile_->Write(batch);
  if (options.sync) {
    rwfile_->Sync();
  }
  if (rwfile_->FileSize() > options_.max_vlog_file_size) {
    WriteLock l(&rwlock_);
    FinishBuild();
    s = NewVLogBuilder();
  }
  return s;
}

Status ValueLogImpl::Get(const ReadOptions& options, const ValueHandle& handle,
                         std::string* value) {
  ReadLock l(&rwlock_);
  if (handle.table_ == 0 || handle.table_ > CurrentFileNumber()) {
    return Status::InvalidArgument("invalid value log number");
  }
  Iterator* iter;
  if (handle.table_ != CurrentFileNumber()) {
    auto it = ro_files_.find(handle.table_);
    if (it != ro_files_.end()) {
      if (handle.offset_ >= it->second->file_size) {
        return Status::InvalidArgument("value log offset is out of limit");
      }
      iter = vlog_cache_->NewIterator(options, handle.table_,
                                      it->second->file_size);
    } else {
      return Status::InvalidArgument("value log number not exists");
    }
  } else {
    if (handle.offset_ >= rwfile_->Offset()) {
      Log(options_.info_log, "value log offset %u is out of limit %u",
          handle.offset_, rwfile_->Offset());
      return Status::InvalidArgument("value log offset is out of limit");
    }
    iter = rwfile_->NewIterator(options);
  }
  {
    // unlock when reading from disk
    rwlock_.RUnlock();
    std::string handle_encoding;
    handle.EncodeTo(&handle_encoding);
    iter->Seek(handle_encoding);  // expensive
    if (iter->Valid()) {
      const Slice v = iter->value();
      value->assign(v.data(), v.size());
    } else {
      delete iter;
      return Status::NotFound("value not found");
    }
    delete iter;
    rwlock_.RLock();
  }
  return Status::OK();
}

/*
 * 1. A process crash before syncing won't hurt. The filesystem will flush the
 * dirty page to the disk.
 * 2. An OS crash before DB::Put() return won't hurt.
 * 3. An OS crash between VLog::Put() and LSM::Put() won't hurt.
 * 3. An OS crash after DB::Put(sync=true) won't hurt.
 * 5. An OS crash after DB::Put(sync=false) will cause last few records lost.
 */
Status ValueLogImpl::Recover() {
  WriteLock l(&rwlock_);
  Status s;
  std::vector<std::string> fnames;
  uint64_t number;
  FileType type;

  // scan over all the .vlog files
  env_->GetChildren(dbname_, &fnames);
  for (std::string& filename : fnames) {
    if (ParseFileName(filename, &number, &type) && type == kVLogFile) {
      vlog_file_number_ = std::max(number, vlog_file_number_);
      VLogFileMeta* f = new VLogFileMeta;
      f->number = number;
      s = env_->GetFileSize(DBFilePath(dbname_, filename), &f->file_size);
      Log(options_.info_log, "filename %s: %s", filename.c_str(),
          s.ToString().c_str());
      assert(s.ok());
      ro_files_.emplace(number, f);
      lock_table_.emplace(number, new port::RWSpinLock);
    }
  }

  // try to reuse the latest file
  uint64_t offset, num_entries;
  bool reuse = false;
  AppendableRandomAccessFile* file;
  VLogReader* reader;
  if (vlog_file_number_ != 0) {
    std::string filename(VLogFileName(dbname_, vlog_file_number_));
    s = env_->NewAppendableRandomAccessFile(filename, &file);
    assert(s.ok());
    s = VLogReader::Open(options_, file,
                         ro_files_[vlog_file_number_]->file_size, &reader);

    Log(options_.info_log, "validating %s, file_size=%llu", filename.c_str(),
        ro_files_[vlog_file_number_]->file_size);

    bool valid = reader->Validate(&offset, &num_entries);
    /*
     * it's not expensive to reopen and re-create once more when recovering a
     * database, we delete them here and re-create later for better code
     * readability.
     */
    delete reader;
    delete file;

    if (!valid) {
      /*
       * The file's valid size is smaller than its actual size, repair it by
       * truncating it to the valid size.
       */
      s = env_->TruncateFile(filename, offset);
      Log(options_.info_log, "%s broken, truncating it from %llu to %llu",
          filename.c_str(), ro_files_[vlog_file_number_]->file_size, offset);
    }

    if (offset <= (options_.max_vlog_file_size / 2)) {
      /*
       * reuse the .vlog file
       */
      Log(options_.info_log, "reusing %s, ValidSize/FileSize = %llu/%llu",
          filename.c_str(), offset, ro_files_[vlog_file_number_]->file_size);
      reuse = true;
      delete ro_files_[vlog_file_number_];
      delete lock_table_[vlog_file_number_];
      ro_files_.erase(vlog_file_number_);
      lock_table_.erase(vlog_file_number_);
    }
#ifndef NDEBUG
    assert(s.ok());
    uint64_t file_size;
    env_->GetFileSize(filename, &file_size);
    assert(offset == file_size);
#endif
  }

  if (!reuse) {
    NewFileNumber();
    offset = 0;
    num_entries = 0;
  }

  s = env_->NewAppendableRandomAccessFile(
      VLogFileName(dbname_, CurrentFileNumber()), &file);
  assert(s.ok());
  rwfile_ = new VLogRWFile(options_, file, reuse, offset, num_entries);
  rwfile_->Ref();
  lock_table_.emplace(CurrentFileNumber(), new port::RWSpinLock);

  return s;
}

ValueLogImpl::~ValueLogImpl() {
  FinishBuild();
  rwfile_->Unref();
  shutdown_.store(true, std::memory_order_release);
  for (auto p : ro_files_) {
    p.second->refs--;
    if (p.second->refs <= 0) {
      delete p.second;
    }
  }
  delete vlog_cache_;
}

Status ValueLogImpl::FinishBuild() {
  rwlock_.AssertWLockHeld();
  rwfile_->Finish();
  VLogFileMeta* ro_f = new VLogFileMeta;
  ro_f->file_size = rwfile_->FileSize();
  ro_f->number = CurrentFileNumber();

  ro_files_.emplace(ro_f->number, ro_f);
  return Status::OK();
}

Status ValueLogImpl::NewVLogBuilder() {
  rwlock_.AssertWLockHeld();
  Status s;
  rwfile_->Unref();

  AppendableRandomAccessFile* file;
  s = env_->NewAppendableRandomAccessFile(
      VLogFileName(dbname_, NewFileNumber()), &file);
  rwfile_ = new VLogRWFile(options_, file);
  rwfile_->Ref();
  lock_table_.emplace(CurrentFileNumber(), new port::RWSpinLock);
  return s;
}

Iterator* ValueLogImpl::NewVLogFileIterator(const ReadOptions& options, uint64_t number) {
  rwlock_.AssertRLockHeld();
  if (number == CurrentFileNumber()) {
    return rwfile_->NewIterator(options);
  } else {
    auto it = ro_files_.find(number);
    if (it != ro_files_.end()) {
      return vlog_cache_->NewIterator(options, number, it->second->file_size);
    }
  }
  return nullptr;
}

void ValueLogImpl::BGWork(void* vlog) {
  reinterpret_cast<ValueLogImpl*>(vlog)->BackgroundGC();
}

void ValueLogImpl::BackgroundGC() {
  ReadLock l(&rwlock_);
  uint64_t number = PickGC();
  Iterator* iter = NewVLogFileIterator(ReadOptions(), number);

  uint64_t seq;
  Slice ikey, key;
  for (iter->SeekToFirst(); iter->Valid() ; iter->Next()) {
    ikey = iter->key();
    size_t key_size = DecodeInternalKey(ikey.data(),
                                       ikey.data()+ikey.size(), &seq);
    key = Slice(ikey.data(), key_size);
  }
}

uint64_t ValueLogImpl::PickGC() {
  rwlock_.AssertRLockHeld();
  gc_ptr_++;
  if (gc_ptr_ == CurrentFileNumber()){
    gc_ptr_ = 1;
  }
  return gc_ptr_;
}

static std::string Bytes2Size(uint64_t size) {
  char buf[100];
  if (size < (1 << 10)) {
    snprintf(buf, sizeof(buf), "%llu B", size);
    return {buf};
  } else if (size < (1 << 20)) {
    snprintf(buf, sizeof(buf), "%.2f KB", double(size) / 1024.0);
  } else if (size < (1 << 30)) {
    snprintf(buf, sizeof(buf), "%.2f MB", double(size) / (1024.0 * 1024.0));
  } else {
    snprintf(buf, sizeof(buf), "%.2f GB",
             double(size) / (1024.0 * 1024.0 * 1024.0));
  }
  return buf;
}

std::string ValueLogImpl::DebugString() {
  ReadLock l(&rwlock_);
  std::string s = "=========Value Log==========\n";
  s.append(VLogFileName("", CurrentFileNumber()).erase(0, 1) + ":\t" +
           Bytes2Size(rwfile_->FileSize()) + " (current)\n");
  for (auto f : ro_files_) {
    s.append(VLogFileName("", f.second->number).erase(0, 1) + ":\t" +
             Bytes2Size(f.second->file_size) + "\n");
  }
  return s;
}

}  // namespace leveldb