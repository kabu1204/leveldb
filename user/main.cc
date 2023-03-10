//
// Created by 于承业 on 2023/3/10.
//
#include "util/testutil.h"
#include "leveldb/db.h"
#include "main.h"

int main(){
  leveldb::DB* db;
  leveldb::Options options;
  leveldb::WriteOptions woption;
  options.create_if_missing = true;
  options.info_log = new StdLogger;
  leveldb::Status status = leveldb::DB::Open(options, "testdb", &db);
  assert(status.ok());

  for(int i=0; i<1000000; ++i){
    db->Put(woption, "my_key"+std::to_string(i), "my_value"+std::to_string(i));
  }

  std::string stats;
  db->GetProperty("leveldb.stats", &stats);
  std::printf("STATS:\n%s\n", stats.c_str());

  delete db;
  leveldb::DestroyDB("testdb", options);
  return 0;
}