//
// Created by 于承业 on 2023/4/17.
//

#ifndef LEVELDB_SYNC_POINT_H
#define LEVELDB_SYNC_POINT_H

#include <unordered_map>

#include "leveldb/status.h"

#include "port/port_stdcxx.h"
#include "util/mutexlock.h"

namespace leveldb {
namespace test {

class SyncPoint {
 public:
  using CallbackFn = bool (*)(void*);

  /*
   * if callback return true, the function return at the sync point;
   */
  bool Sync(std::string&& name) {
    CallbackFn cb = nullptr;
    void* arg = nullptr;
    lk_.RLock();
    if (sync_points_.find(name) != sync_points_.end()) {
      cb = sync_points_[name].first;
      arg = sync_points_[name].second;
    }
    lk_.RUnlock();
    if (cb != nullptr) {
      return cb(arg);
    }
    return false;
  }

  static SyncPoint* GetInstance() {
    static SyncPoint point;
    return &point;
  }

  static SyncPoint* New(std::string&& name, CallbackFn cb = nullptr,
                        void* arg = nullptr) {
    SyncPoint* point = GetInstance();
    WriteLock l(&point->lk_);
    if (point->sync_points_.find(name) == point->sync_points_.end()) {
      point->sync_points_.emplace(std::move(name), std::make_pair(cb, arg));
    }
    return point;
  }

  void SetArg(std::string&& name, void* arg) {
    WriteLock l(&lk_);
    sync_points_[name].second = arg;
  }

  void SetCallback(std::string&& name, CallbackFn cb) {
    WriteLock l(&lk_);
    sync_points_[name].first = cb;
  }

 private:
  SyncPoint() = default;

  port::RWSpinLock lk_;
  std::unordered_map<std::string, std::pair<CallbackFn, void*>> sync_points_
      GUARDED_BY(lk_);
};

#ifndef NDEBUG
#define TEST_SYNC_POINT(name)                            \
  do {                                                   \
    leveldb::test::SyncPoint::New((name))->Sync((name)); \
  } while (0);

#define TEST_SYNC_POINT_MAY_RETURN(name, ret)                  \
  do {                                                         \
    if (leveldb::test::SyncPoint::New((name))->Sync((name))) { \
      return (ret);                                            \
    }                                                          \
  } while (0);

#define TEST_SYNC_POINT_CALLBACK(name, cb) \
  leveldb::test::SyncPoint::GetInstance()->SetCallback((name), ((cb)))
#define TEST_SYNC_POINT_ARG(name, arg) \
  leveldb::test::SyncPoint::GetInstance()->SetArg((name), (arg))
#define TEST_SYNC_POINT_CLEAR(name)                                        \
  do {                                                                     \
    leveldb::test::SyncPoint::GetInstance()->SetArg((name), nullptr);      \
    leveldb::test::SyncPoint::GetInstance()->SetCallback((name), nullptr); \
  } while (0);
#else
#define TEST_SYNC_POINT(name)
#define TEST_SYNC_POINT_CALLBACK(name, cb)
#define TEST_SYNC_POINT_ARG(name, arg)
#endif

}  // namespace test
}  // namespace leveldb

#endif  // LEVELDB_SYNC_POINT_H
