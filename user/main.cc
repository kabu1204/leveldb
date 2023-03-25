//
// Created by 于承业 on 2023/3/10.
//
#include "util/testutil.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "util/coding.h"
#include "main.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

void db_sample(){
  leveldb::DB* db;
  leveldb::Options options;
  leveldb::WriteOptions woption;
  options.create_if_missing = true;
  options.info_log = new StdLogger();
  leveldb::Status status =
      leveldb::DB::Open(options, "testdb", &db);

  for(int i=0; i<1000000; ++i){
    db->Put(woption, "my_key"+std::to_string(i), "my_value"+std::to_string(i));
  }

  std::string stats;
  db->GetProperty("leveldb.stats", &stats);
  std::printf("STATS:\n%s\n", stats.c_str());

  delete db;
  leveldb::DestroyDB("testdb", options);
}

uint64_t AlignBackward(uint64_t x, uint64_t p){
  if(x & (p-1))
    x = (x+p) & (~(p-1));
  return x;
}

int get_file_size(const std::string& filename, uint64_t* size){
  struct ::stat file_stat;
  if (::stat(filename.c_str(), &file_stat) != 0) {
    *size = 0;
    return -1;
  }
  *size = file_stat.st_size;
  return 0;
}

void mmap_test(){
  const std::string filename = "mmap_test_file";
  const size_t page_size = 16384;
  char* base = (char*)mmap(NULL, 1U << 20, PROT_NONE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  if(base == MAP_FAILED) {
    printf("mmap failed\n");
    return;
  }

  int fd = ::open(filename.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_APPEND, 0644);
  if (fd < 0) {
    printf("open failed\n");
    return;
  }

  if(ftruncate(fd, page_size)<0){
    printf("truncate failed\n");
    return;
  }

  uint64_t file_size;
  if(get_file_size(filename, &file_size)<0){
    printf("get_file_size failed\n");
    return;
  }

  // map file to the start addr
  char* addr = (char*)mmap(base, file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_FIXED, fd, 0);
  if(addr==MAP_FAILED || addr != base){
    printf("base: %p\n", base);
    printf("addr: %p\n", addr);
    printf("mmap file failed %llu %d\n", file_size, errno==EACCES);
    return;
  }
  printf("base: %p\n", base);

  if(ftruncate(fd, page_size<<1)<0){
    printf("truncate failed\n");
    return;
  }

  if(get_file_size(filename, &file_size)<0){
    printf("get_file_size failed\n");
    return;
  }

  if(file_size!=page_size*2){
    printf("err file size: %llu\n", file_size);
    return ;
  }

  // grow file mapping area by a page_size
  char *addr2 = (char*)mmap(base + page_size, page_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_FIXED, fd, page_size);
  if(addr2==MAP_FAILED || addr2 != base+page_size){
    printf("base+4096: %p\n", base+page_size);
    printf("addr2: %p\n", addr2);
    printf("mmap file failed %llu %d\n", file_size, errno==EINVAL);
    return;
  }

  close(fd);
  munmap(base, 1U<<20);
}

void PosixMmapAppendableFileTest(){
  AppendableRandomAccessFile* file;
  const std::string fname = "mmap_appendable_file";
  Status s = Env::Default()->NewAppendableRandomAccessFile(fname, &file);
  if(!s.ok()){
    printf("%s\n", s.ToString().c_str());
    return;
  }

  Slice result;
  char buf[64];

  for(uint64_t i=0; i<100000; i++) {
    char enc[sizeof(uint64_t)];
    EncodeFixed64(enc, i);
    s = file->Append(Slice(enc, sizeof(enc)));
    if (!s.ok()) { printf("%s\n", s.ToString().c_str()); return; }
  }

  for(uint64_t i=0; i<100000; i++){
    s = file->Read(sizeof(uint64_t)*i, sizeof(uint64_t), &result, buf);
    if (!s.ok()) { printf("%s\n", s.ToString().c_str()); return; }
    assert(DecodeFixed64(result.data())==i);
  }

  delete file;
  Env::Default()->RemoveFile(fname);
}

int main(){
  PosixMmapAppendableFileTest();
  return 0;
}