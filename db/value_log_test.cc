//
// Created by 于承业 on 2023/4/1.
//

#include "leveldb/value_log.h"

#include "db/filename.h"
#include "db/value_log_impl.h"
#include <thread>

#include "leveldb/env.h"

#include "table/format.h"

#include "gtest/gtest.h"

using namespace leveldb;

static void CleanDir(Env* const env, const std::string& dir) {
  std::vector<std::string> fnames;
  env->GetChildren(dir, &fnames);
  for (auto& filename : fnames) {
    env->RemoveFile(DBFilePath(dir, filename));
  }
  env->RemoveDir(dir);
}

uint32_t SizeOfVariant32(uint32_t v) {
  uint32_t size = 1;
  while (v >>= 7) {
    size++;
  }
  return size;
}

uint64_t SizeOf(const Slice& key, const Slice& val) {
  return SizeOfVariant32(key.size()) + SizeOfVariant32(val.size()) +
         key.size() + val.size();
}

TEST(VLOG_TEST, Recover) {
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.max_vlog_file_size = 8 << 20;
  std::string dbname("testdb");
  DB* db;
  DB::Open(options, dbname, &db);
  ValueLog* v;
  ValueLog::Open(options, dbname, &v);

  ValueHandle handle;
  v->Add(WriteOptions(), "k01", "value01", &handle);
  assert(handle == ValueHandle(1, 0, 0, 12));
  v->Add(WriteOptions(), "k02", "value02", &handle);
  assert(handle == ValueHandle(1, 0, 12, 12));
  v->Add(WriteOptions(), "k03", "value03", &handle);
  assert(handle == ValueHandle(1, 0, 24, 12));

  delete v;

  ValueLog::Open(options, dbname, &v);

  std::string value;
  v->Get(ReadOptions(), ValueHandle(1, 0, 0, 12), &value);
  ASSERT_EQ(value, "value01");
  v->Get(ReadOptions(), ValueHandle(1, 0, 12, 12), &value);
  ASSERT_EQ(value, "value02");
  v->Get(ReadOptions(), ValueHandle(1, 0, 24, 0), &value);
  ASSERT_EQ(value, "value03");

  v->Add(WriteOptions(), "k04", "value04", &handle);
  ASSERT_EQ(handle, ValueHandle(1, 0, 36, 12));
  v->Add(WriteOptions(), "k05", "value05", &handle);
  ASSERT_EQ(handle, ValueHandle(1, 0, 48, 12));
  v->Add(WriteOptions(), "k06", "value06", &handle);
  ASSERT_EQ(handle, ValueHandle(1, 0, 60, 12));

  // simulate broken .vlog file with last few records lost caused by OS crash
  for (int i = 60; i < 72; i++) {
    delete v;
    options.env->TruncateFile(VLogFileName(dbname, 1), i);
    ValueLog::Open(options, dbname, &v);

    v->Add(WriteOptions(), "k06", "value06", &handle);
    ASSERT_EQ(handle, ValueHandle(1, 0, 60, 12));
  }

  uint32_t size = 72;
  uint32_t num_entries = 6;
  for (int i = 0; size <= options.max_vlog_file_size / 2; i++) {
    Slice key("k0" + std::to_string(i + 7));
    Slice val("value0" + std::to_string(i + 7));
    v->Add(WriteOptions(), key, val, &handle);
    ASSERT_EQ(handle, ValueHandle(1, 0, size, SizeOf(key, val)));
    size += SizeOf(key, val);
    num_entries++;
  }

  delete v;
  ValueLog::Open(options, dbname, &v);

  size = 0;
  for (int i = 1; i <= num_entries; i++) {
    Slice key("k1" + std::to_string(i));
    Slice val("value1" + std::to_string(i));
    v->Add(WriteOptions(), key, val, &handle);
    ASSERT_EQ(handle, ValueHandle(2, 0, size, SizeOf(key, val)));
    size += SizeOf(key, val);
  }

  size = 0;
  for (int i = 1; i <= num_entries; i++) {
    Slice key("k0" + std::to_string(i));
    Slice val("value0" + std::to_string(i));
    s = v->Get(ReadOptions(), ValueHandle(1, 0, size, SizeOf(key, val)),
               &value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(Slice(value), val);
    size += SizeOf(key, val);
  }

  size = 0;
  for (int i = 1; i <= 1000000; i++) {
    Slice key("k1" + std::to_string(i));
    Slice val("value1" + std::to_string(i));
    s = v->Add(WriteOptions(), key, val, &handle);
    ASSERT_TRUE(s.ok());
    size += SizeOf(key, val);
  }

  printf("%s", v->DebugString().c_str());

  delete v;
  delete db;
  CleanDir(options.env, dbname);
}

TEST(VLOG_TEST, Concurrent) {
  Options options;
  Status s;
  options.env->NewStdLogger(&options.info_log);
  options.create_if_missing = true;
  options.max_vlog_file_size = 8 << 20;
  std::string dbname("testdb");
  DB* db;
  DB::Open(options, dbname, &db);
  ValueLog* v;
  ValueLog::Open(options, dbname, &v);
  std::deque<std::pair<ValueHandle, std::string>> kvq;
  port::Mutex lk;
  port::CondVar cv(&lk);

  std::thread* wth[8];
  std::thread* rth[8];

  for (int i = 0; i < 8; i++) {
    wth[i] = new std::thread(
        [&lk, &kvq, &v, &cv](int k) {
          ValueHandle handle_;
          for (int j = k * 100000; j < (k + 1) * 100000; ++j) {
            std::string key("k0" + std::to_string(j + 7));
            std::string val("value0" + std::to_string(j + 7));
            auto s = v->Add(WriteOptions(), key, val, &handle_);
            ASSERT_TRUE(s.ok());
            lk.Lock();
            kvq.emplace_back(handle_, val);
            lk.Unlock();
            cv.SignalAll();
          }
          return;
        },
        i);
    rth[i] = new std::thread(
        [&lk, &kvq, &v, &cv](int k) {
          ValueHandle handle_;
          std::string val;
          std::string expected;
          for (int j = k * 100000; j < (k + 1) * 100000; ++j) {
            lk.Lock();
            while (kvq.empty()) {
              cv.Wait();
            }
            handle_ = kvq.front().first;
            expected = kvq.front().second;
            kvq.pop_front();
            lk.Unlock();
            auto s = v->Get(ReadOptions(), handle_, &val);
            ASSERT_TRUE(s.ok());
            ASSERT_EQ(val, expected);
          }
          return;
        },
        i);
  }

  for (int i = 0; i < 8; i++) {
    wth[i]->join();
    rth[i]->join();
  }

  printf("%s", v->DebugString().c_str());

  delete v;
  delete db;
  CleanDir(options.env, dbname);
}