//
// Created by 于承业 on 2023/4/19.
//

#include "threadpool_impl.h"

namespace leveldb {

void ThreadPoolImpl::JoinAll(bool wait_complete) {
  MutexLock l(&mutex_);
  assert(!exit_all_);

  exit_all_ = true;
  wait_complete_exit_all_ = wait_complete;
  cv_.SignalAll();

  for (auto& th : threads_) {
    th.join();
  }

  threads_.clear();

  exit_all_ = false;
  wait_complete_exit_all_ = false;
}

void ThreadPoolImpl::JoinAllThreads() { JoinAll(false); }

void ThreadPoolImpl::SetBackgroundThreads(int num) {
  MutexLock l(&mutex_);
  if (num > max_bg_threads_) {
    max_bg_threads_ = num;
  }
}

int ThreadPoolImpl::GetBackgroundThreads() {
  MutexLock l(&mutex_);
  return max_bg_threads_;
}

unsigned int ThreadPoolImpl::GetQueueLen() const {
  return num_jobs.load(std::memory_order_acquire);
}

void ThreadPoolImpl::WaitForJobsAndJoinAllThreads() { JoinAll(true); }

void ThreadPoolImpl::SubmitJob(const std::function<void()>& job) {
  Submit(std::function<void()>(job));  // copy and move
}

void ThreadPoolImpl::SubmitJob(std::function<void()>&& job) {
  Submit(std::move(job));  // move
}

void ThreadPoolImpl::Schedule(void (*func)(void*), void* arg) {
  assert(func != nullptr);
  SubmitJob(std::bind(func, arg));
}

void ThreadPoolImpl::BGEntry(ThreadPoolImpl* pool) { pool->BGLoop(); }

void ThreadPoolImpl::BGLoop() {
  while (true) {
    mutex_.Lock();
    num_waiting_threads_++;

    while (!exit_all_ && jobs_.empty()) {
      cv_.Wait();
    }

    num_waiting_threads_--;

    if (exit_all_) {
      if (!wait_complete_exit_all_ || jobs_.empty()) {
        mutex_.Unlock();
        break;
      }
    }

    std::function<void()> func = std::move(jobs_.front());
    jobs_.pop_front();

    num_jobs.fetch_sub(1);

    mutex_.Unlock();
    func();
  }
}

void ThreadPoolImpl::StartBGThreads() {
  mutex_.AssertHeld();
  assert(max_bg_threads_ > 0);
  while (threads_.size() < max_bg_threads_) {
    std::thread th(&BGEntry, this);
    threads_.emplace_back(std::move(th));
  }
}

void ThreadPoolImpl::Submit(std::function<void()>&& job) {
  MutexLock l(&mutex_);

  StartBGThreads();

  jobs_.emplace_back(std::move(job));
  num_jobs.fetch_add(1);

  cv_.Signal();
}

}  // namespace leveldb