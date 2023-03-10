//
// Created by 于承业 on 2023/3/10.
//

#ifndef LEVELDB_MAIN_H
#define LEVELDB_MAIN_H

#include "leveldb/env.h"
#include <cstdio>

using namespace leveldb;

class StdLogger: public Logger {
 public:
  StdLogger()=default;
  ~StdLogger()=default;

  void Logv(const char* format, std::va_list ap){
    std::va_list ap_copy;
    va_copy(ap_copy, ap);
    std::printf("[INFO] ");
    std::vprintf(format, ap);
    std::printf("\n");
    va_end(ap_copy);
  }
};

#endif  // LEVELDB_MAIN_H
