//
// Created by 于承业 on 2023/4/1.
//

#include "gtest/gtest.h"
#include "db/value_log.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "table/format.h"

using namespace leveldb;

static void CleanDir(Env* const env, const std::string& dir) {
  std::vector<std::string> fnames;
  env->GetChildren(dir, &fnames);
  for (auto &filename : fnames) {
    env->RemoveFile(DBFilePath(dir, filename));
  }
  env->RemoveDir(dir);
}

TEST(VLOG_TEST, Sample){
  Options option;
  Status s;
  option.env->NewStdLogger(&option.info_log);
  option.create_if_missing = true;
  std::string dbname("testdb");
  DB* db;
  DB::Open(option, dbname, &db);
  ValueLog *v = new ValueLog(option, dbname);
  v->Recover();

  ValueHandle handle;
  v->Add(WriteOptions(), "k01", "value01", &handle);
  assert(handle == ValueHandle(1, 0, 0, 12));
  v->Add(WriteOptions(), "k02", "value02", &handle);
  assert(handle == ValueHandle(1, 0, 12, 12));
  v->Add(WriteOptions(), "k03", "value03", &handle);
  assert(handle == ValueHandle(1, 0, 24, 12));

  delete v;

  v = new ValueLog(option, dbname);
  v->Recover();

  std::string value;
  s = v->Get(ReadOptions(), ValueHandle(1, 0, 0, 12), &value);
  printf("s: %s\n",s.ToString().c_str());
  ASSERT_EQ(value, "value01");
  v->Get(ReadOptions(), ValueHandle(1, 0, 12, 12), &value);
  ASSERT_EQ(value, "value02");
  v->Get(ReadOptions(), ValueHandle(1, 0, 24, 0), &value);
  ASSERT_EQ(value, "value03");

  v->Add(WriteOptions(), "k04", "value04", &handle);
  assert(handle == ValueHandle(1, 0, 36, 12));
  v->Add(WriteOptions(), "k05", "value05", &handle);
  assert(handle == ValueHandle(1, 0, 48, 12));
  v->Add(WriteOptions(), "k06", "value06", &handle);
  assert(handle == ValueHandle(1, 0, 60, 12));

  delete v;

  // simulate broken .vlog file with last few records lost caused by OS crash
  for(int i=60; i<72; i++) {
    option.env->TruncateFile(VLogFileName(dbname, 1), i);
    v = new ValueLog(option, dbname);
    v->Recover();

    v->Add(WriteOptions(), "k06", "value06", &handle);
    assert(handle == ValueHandle(1, 0, 60, 12));
    delete v;
  }

  delete db;
  CleanDir(option.env, dbname);
}