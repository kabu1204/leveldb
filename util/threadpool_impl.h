// Copyright (c) 2023. Chengye YU <yuchengye2013 AT outlook.com>
// SPDX-License-Identifier: BSD-3-Clause

#ifndef LEVELDB_THREADPOOL_IMPL_H
#define LEVELDB_THREADPOOL_IMPL_H

#include <deque>
#include <functional>
#include <vector>

#include "leveldb/status.h"
#include "leveldb/threadpool.h"

#include "port/port_stdcxx.h"
#include "util/mutexlock.h"

namespace leveldb {

class ThreadPoolImpl : public ThreadPool {
 public:
  ThreadPoolImpl()
      : max_bg_threads_(0),
        num_jobs(0),
        num_waiting_threads_(0),
        exit_all_(false),
        wait_complete_exit_all_(false),
        cv_(&mutex_) {}

  ~ThreadPoolImpl() override = default;

  ThreadPoolImpl(const ThreadPoolImpl&) = delete;
  ThreadPoolImpl& operator=(const ThreadPoolImpl&) = delete;

  // Wait for all threads to finish.
  // Discard those threads that did not start
  // executing
  void JoinAllThreads() override;

  // Set the number of background threads that will be executing the
  // scheduled jobs.
  void SetBackgroundThreads(int num) override;
  int GetBackgroundThreads() override;

  // Get the number of jobs scheduled in the ThreadPool queue.
  unsigned int GetQueueLen() const override;

  // Waits for all jobs to complete those
  // that already started running and those that did not
  // start yet. This ensures that everything that was thrown
  // on the TP runs even though
  // we may not have specified enough threads for the amount
  // of jobs
  void WaitForJobsAndJoinAllThreads() override;

  // Submit a fire and forget jobs
  // This allows to submit the same job multiple times
  void SubmitJob(const std::function<void()>&) override;
  // This moves the function in for efficiency
  void SubmitJob(std::function<void()>&&) override;

  void Schedule(void (*func)(void*), void* arg);

 private:
  static void BGEntry(ThreadPoolImpl* pool);

  void BGLoop();

  void StartBGThreads() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void JoinAll(bool wait_complete = false);

  void Submit(std::function<void()>&&);

  port::Mutex mutex_;
  port::CondVar cv_ GUARDED_BY(mutex_);
  int max_bg_threads_ GUARDED_BY(mutex_);
  int num_waiting_threads_ GUARDED_BY(mutex_);
  std::deque<std::function<void()>> jobs_ GUARDED_BY(mutex_);
  std::vector<std::thread> threads_;
  std::atomic<size_t> num_jobs;
  bool exit_all_;
  bool wait_complete_exit_all_;
};

}  // namespace leveldb

#endif  // LEVELDB_THREADPOOL_IMPL_H
