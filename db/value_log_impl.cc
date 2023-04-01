//
// Created by 于承业 on 2023/3/22.
//

#include "db/value_log_impl.h"

#include "db/filename.h"

#include "table/format.h"

namespace leveldb {

ValueLog::~ValueLog() = default;

Status ValueLog::Open(const Options& options, const std::string& dbname,
                      ValueLog** vlog) {
  if (vlog != nullptr) {
    *vlog = nullptr;
  }

  ValueLogImpl* vlog_impl = new ValueLogImpl(options, dbname);
  vlog_impl->Recover();
  *vlog = vlog_impl;

  return Status::OK();
}

ValueLogImpl::ValueLogImpl(const Options& options, const std::string& dbname)
    : env_(options.env),
      options_(options),
      dbname_(dbname),
      vlog_cache_(new VLogCache(dbname_, options, options.max_open_vlogs)),
      shutdown_(false),
      rw_file_(nullptr),
      builder_(nullptr),
      reader_(nullptr),
      vlog_file_number_(0) {}

Status ValueLogImpl::Add(const WriteOptions& options, const Slice& key,
                         const Slice& value, ValueHandle* handle) {
  Status s;
  handle->table_ = CurrentFileNumber();
  builder_->Add(key, value, handle);
  builder_->Flush();
  if (options.sync) {
    s = rw_file_->Sync();
  }
  reader_->IncreaseOffset(builder_->Offset());
  if (builder_->FileSize() > options_.max_vlog_file_size) {
    FinishBuild();
    s = NewVLogBuider();
  }
  return s;
}

Status ValueLogImpl::Get(const ReadOptions& options, const ValueHandle& handle,
                         std::string* value) {
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
      iter = vlog_cache_->NewIterator(options, handle.table_, it->second->file_size);
    } else {
      return Status::InvalidArgument("value log number not exists");
    }
  } else {
    if(handle.offset_ >= builder_->Offset()){
      return Status::InvalidArgument("value log offset is out of limit");
    }
    iter = reader_->NewIterator(options);
  }
  std::string handle_encoding;
  handle.EncodeTo(&handle_encoding);
  iter->Seek(handle_encoding);
  if (iter->Valid()) {
    const Slice v = iter->value();
    value->assign(v.data(), v.size());
  } else {
    delete iter;
    return Status::NotFound("value not found");
  }
  delete iter;
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
      Log(options_.info_log, "filename %s: %s", filename.c_str(), s.ToString().c_str());
      assert(s.ok());
      ro_files_.emplace(number, f);
    }
  }

  // try to reuse the latest file
  uint64_t offset, num_entries;
  bool reuse = false;
  if (vlog_file_number_ != 0) {
    std::string filename(VLogFileName(dbname_, vlog_file_number_));
    s = env_->NewAppendableRandomAccessFile(filename, &rw_file_);
    assert(s.ok());
    s = VLogReader::Open(options_, rw_file_,
                         ro_files_[vlog_file_number_]->file_size, &reader_);

    Log(options_.info_log, "validating %s, file_size=%llu", filename.c_str(),
        ro_files_[vlog_file_number_]->file_size);

    bool valid = reader_->Validate(&offset, &num_entries);
    /*
     * it's not expensive to reopen and re-create once more when recovering a
     * database, we delete them here and re-create later for better code readability.
     */
    delete reader_;
    delete rw_file_;

    if(!valid){
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
      ro_files_.erase(vlog_file_number_);
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

  s = env_->NewAppendableRandomAccessFile(VLogFileName(dbname_, CurrentFileNumber()), &rw_file_);
  assert(s.ok());
  builder_ = new VLogBuilder(options_, rw_file_, reuse, offset, num_entries);
  s = VLogReader::Open(options_, rw_file_, offset, &reader_);

  return s;
}

ValueLogImpl::~ValueLogImpl() {
  FinishBuild();
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
  VLogFileMeta* ro_f = new VLogFileMeta;
  builder_->Finish();
  rw_file_->Sync();
  ro_f->file_size = builder_->FileSize();
  ro_f->number = CurrentFileNumber();
  delete reader_;
  delete builder_;
  rw_file_->Close();
  delete rw_file_;
  rw_file_ = nullptr;
  builder_ = nullptr;
  reader_ = nullptr;

  ro_files_.emplace(ro_f->number, ro_f);
  return Status::OK();
}

Status ValueLogImpl::NewVLogBuider() {
  Status s;
  s = env_->NewAppendableRandomAccessFile(
      VLogFileName(dbname_, NewFileNumber()), &rw_file_);
  builder_ = new VLogBuilder(options_, rw_file_);
  s = VLogReader::Open(options_, rw_file_, 0, &reader_);
  return s;
}

static std::string Bytes2Size(uint64_t size) {
  /*
   * [0, 1024B] => B
   * [1KB, 1MB] => KB
   *
   */
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
  std::string s = "=========Value Log==========\n";
  s.append(VLogFileName("", CurrentFileNumber()).erase(0, 1) + ":\t" +
           Bytes2Size(builder_->FileSize()) + " (current)\n");
  for (auto f : ro_files_) {
    s.append(VLogFileName("", f.second->number).erase(0, 1) + ":\t" +
             Bytes2Size(f.second->file_size) + "\n");
  }
  return s;
}

}  // namespace leveldb