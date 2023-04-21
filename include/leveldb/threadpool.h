// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#ifndef LEVELDB_THREADPOOL_H
#define LEVELDB_THREADPOOL_H

#include <future>

#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

class LEVELDB_EXPORT ThreadPool {
 public:
  ThreadPool() = default;

  virtual ~ThreadPool() = default;

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  // Wait for all threads to finish.
  // Discard those threads that did not start
  // executing
  virtual void JoinAllThreads() = 0;

  // Set the number of background threads that will be executing the
  // scheduled jobs.
  virtual void SetBackgroundThreads(int num) = 0;
  virtual int GetBackgroundThreads() = 0;

  // Get the number of jobs scheduled in the ThreadPool queue.
  virtual unsigned int GetQueueLen() const = 0;

  // Waits for all jobs to complete those
  // that already started running and those that did not
  // start yet. This ensures that everything that was thrown
  // on the TP runs even though
  // we may not have specified enough threads for the amount
  // of jobs
  virtual void WaitForJobsAndJoinAllThreads() = 0;

  // Submit a fire and forget jobs
  // This allows to submit the same job multiple times
  virtual void SubmitJob(const std::function<void()>&) = 0;
  // This moves the function in for efficiency
  virtual void SubmitJob(std::function<void()>&&) = 0;
};

}  // namespace leveldb

#endif  // LEVELDB_THREADPOOL_H
