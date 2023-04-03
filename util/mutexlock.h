// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
#define STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class SCOPED_LOCKABLE ReadLock {
 public:
  explicit ReadLock(port::RWLock* lk) SHARED_LOCK_FUNCTION(lk) : lk_(lk) {
    this->lk_->RLock();
  }
  ~ReadLock() UNLOCK_FUNCTION() { this->lk_->RUnlock(); }

  ReadLock(const ReadLock&) = delete;
  ReadLock& operator=(const ReadLock&) = delete;

 private:
  port::RWLock* const lk_;
};

class SCOPED_LOCKABLE WriteLock {
 public:
  explicit WriteLock(port::RWLock* lk) EXCLUSIVE_LOCK_FUNCTION(lk) : lk_(lk) {
    this->lk_->WLock();
  }
  ~WriteLock() UNLOCK_FUNCTION() { this->lk_->WUnlock(); }

  WriteLock(const WriteLock&) = delete;
  WriteLock& operator=(const WriteLock&) = delete;

 private:
  port::RWLock* const lk_;
};

class SCOPED_LOCKABLE MutexLock {
 public:
  explicit MutexLock(port::Mutex* mu) EXCLUSIVE_LOCK_FUNCTION(mu) : mu_(mu) {
    this->mu_->Lock();
  }
  ~MutexLock() UNLOCK_FUNCTION() { this->mu_->Unlock(); }

  MutexLock(const MutexLock&) = delete;
  MutexLock& operator=(const MutexLock&) = delete;

 private:
  port::Mutex* const mu_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
