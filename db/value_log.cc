//
// Created by 于承业 on 2023/3/22.
//

#include "db/value_log.h"
#include "db/filename.h"

namespace leveldb {

ValueLog::ValueLog(const Options& options, const std::string& dbname)
    : env_(options.env),
      options_(options),
      dbname_(dbname)
{}

Status ValueLog::Add(const WriteOptions& options, const Slice& key,
                     const Slice& value, ValueHandle* handle) {
  Status s;
  handle->offset_ = builder_->Offset();
  builder_->Add(key, value);
  builder_->Flush();
  if (options.sync) {
    s = rw_file_->Sync();
  }
  reader_->IncreaseOffset(builder_->Offset());
  if(builder_->FileSize() > options_.max_file_size) {
    // TODO: use unref here
    builder_->Finish();
    rw_file_->Sync();
    delete reader_;
    delete builder_;
    rw_file_->Close();
    delete rw_file_;
    rw_file_ = nullptr;
    builder_ = nullptr;
    reader_ = nullptr;
    s = env_->NewAppendableRandomAccessFile(VLogFileName(dbname_, NewFileNumber()), &rw_file_);
    builder_ = new VLogBuilder(options_, rw_file_);
    s = VLogReader::Open(options_, rw_file_, 0, &reader_);
  }
  return s;
}

Status ValueLog::Get(const ReadOptions& options, const ValueHandle& handle,
                     std::string* value) {
  return Status();
}

}  // namespace leveldb