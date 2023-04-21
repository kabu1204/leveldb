// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.

#include "db/db_iter.h"

#include "db/blob_vlog_impl.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include <deque>
#include <future>

#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      std::fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      std::fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter : public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction { kForward, kReverse };

  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
         uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
        bytes_until_read_sampling_(RandomCompactionPeriod()) {}

  DBIter(const DBIter&) = delete;
  DBIter& operator=(const DBIter&) = delete;

  ~DBIter() override { delete iter_; }
  bool Valid() const override { return valid_; }
  Slice key() const override {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  Slice value() const override {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  Status status() const override {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void Next() override;
  void Prev() override;
  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;

  ValueType valueType() const {
    assert(valid_);
    return valueType_;
  }

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Picks the number of bytes that can be read until a compaction is scheduled.
  size_t RandomCompactionPeriod() {
    return rnd_.Uniform(2 * config::kReadBytesPeriod);
  }

  DBImpl* db_;
  ValueType valueType_;  // current key's ValueType
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;
  Status status_;
  std::string saved_key_;    // == current key when direction_==kReverse
  std::string saved_value_;  // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;
  Random rnd_;
  size_t bytes_until_read_sampling_;
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();

  size_t bytes_read = k.size() + iter_->value().size();
  while (bytes_until_read_sampling_ < bytes_read) {
    bytes_until_read_sampling_ += RandomCompactionPeriod();
    db_->RecordReadSample(k);
  }
  assert(bytes_until_read_sampling_ >= bytes_read);
  bytes_until_read_sampling_ -= bytes_read;

  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else {
    // Store in saved_key_ the current key so we skip it below.
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);

    // iter_ is pointing to current key. We can now safely move to the next to
    // avoid checking current key.
    iter_->Next();
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
  }

  FindNextUserEntry(true, &saved_key_);
}

void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          SaveKey(ikey.user_key, skip);
          skipping = true;
          break;
        case kTypeValueHandle:
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
            // Entry hidden
          } else {
            valid_ = true;
            valueType_ = ikey.type;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;
}

void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) <
          0) {
        break;
      }
    }
    direction_ = kReverse;
  }

  FindPrevUserEntry();
}

void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valueType_ = value_type;
    valid_ = true;
  }
}

void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(&saved_key_,
                    ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();
}

class BlobDBIter : public Iterator {
 public:
  BlobDBIter(DBIter* dbiter, ValueLogImpl* vlog, Env* env,
             bool prefetch = false, int max_prefetch = 16)
      : iter_(dbiter),
        vlog_(vlog),
        env_(env),
        isValueHandle_(false),
        prefetch_(prefetch),
        max_prefetch_(max_prefetch),
        forward_(true),
        bg_thread_finished_(false),
        end_(false),
        closed_(false),
        cv_(&mutex_),
        buf_(nullptr) {
    if (prefetch) {
      assert(max_prefetch > 0);
      buf_ = ::operator new(sizeof(std::promise<bool>) * max_prefetch_);
      /*
       * 1              dispatch thread (standalone)
       * max_prefetch   read thread (thread pool)
       */
      env_->StartThread(&BGWork, this);
    }
  }
  BlobDBIter(const BlobDBIter&) = delete;
  BlobDBIter& operator=(const BlobDBIter&) = delete;

  ~BlobDBIter() override {
    MutexLock l(&mutex_);
    if (prefetch_) {
      closed_ = true;
      cv_.Signal();
      while (!bg_thread_finished_) {
        cv_.Wait();
      }

      if (current_.isBlob.valid()) {
        current_.isBlob.wait();
      }
      for (auto& item : fetched_) {
        if (item.isBlob.valid()) {
          item.isBlob.wait();
        }
      }

      fetched_.clear();

      if (buf_) {
        ::operator delete(buf_);
      }
    }
    delete iter_;
  }
  bool Valid() const override {
    if (prefetch_) {
      return current_.valid;
    } else {
      mutex_.AssertHeld();
      return iter_->Valid();
    }
  }
  Slice key() const override {
    assert(Valid());
    if (prefetch_) {
      return current_.key;
    } else {
      mutex_.AssertHeld();
      return iter_->key();
    }
  }
  Slice value() const override {
    assert(Valid());
    if (prefetch_) {
      return current_.value;
    } else {
      mutex_.AssertHeld();
      return isValueHandle_ ? indirect_value_ : iter_->value();
    }
  }

  Status status() const override {
    if (prefetch_) {
      MutexLock l(&mutex_);
      return current_.status;
    }
    mutex_.AssertHeld();
    return iter_->status();
  }

  void Next() override {
    if (prefetch_) {
      if (finished_.empty() || !forward_) {
        /*
         * There's no finished results, we need to reap from the prefetch queue,
         * OR, the iteration direction has changed, we need to clear the
         * prefetch queue.
         */
        MutexLock l(&mutex_);
        GetFromPrefetched(true);
      } else {
        current_ = std::move(finished_.front());
        finished_.pop_front();
      }
    } else {
      mutex_.AssertHeld();
      iter_->Next();
      MaybeIndirectValue();
    }
  }

  void Prev() override {
    assert(Valid());
    if (prefetch_) {
      if (finished_.empty() || forward_) {
        /*
         * There's no finished results, we need to reap from the prefetch queue,
         * OR, the iteration direction has changed, we need to clear the
         * prefetch queue.
         */
        MutexLock l(&mutex_);
        GetFromPrefetched(false);
      } else {
        current_ = std::move(finished_.front());
        finished_.pop_front();
      }
    } else {
      mutex_.AssertHeld();
      iter_->Prev();
      MaybeIndirectValue();
    }
  }

  void Seek(const Slice& target) override {
    MutexLock l(&mutex_);
    iter_->Seek(target);
    if (!prefetch_) {
      MaybeIndirectValue();
    } else {
      end_ = false;
      WaitAndClear();
      GetFromPrefetched(forward_);
    }
  }

  void SeekToFirst() override {
    MutexLock l(&mutex_);
    iter_->SeekToFirst();
    if (!prefetch_) {
      MaybeIndirectValue();
    } else {
      end_ = false;
      WaitAndClear();
      GetFromPrefetched(forward_);
    }
  }

  void SeekToLast() override {
    MutexLock l(&mutex_);
    iter_->SeekToLast();
    if (!prefetch_) {
      MaybeIndirectValue();
    } else {
      end_ = false;
      WaitAndClear();
      GetFromPrefetched(forward_);
    }
  }

 private:
  struct Item {  // information kept for a prefetch
    Item() = default;
    Item(const Item&) = delete;
    Item& operator=(const Item&) = delete;
    Item(Item&& moved) noexcept
        : key(std::move(moved.key)),
          value(std::move(moved.value)),
          isBlob(std::move(moved.isBlob)),
          status(std::move(moved.status)),
          valid(moved.valid),
          forward(moved.forward) {}

    Item& operator=(Item&& moved) noexcept {
      if (this != &moved) {
        valid = moved.valid;
        forward = moved.forward;
        status = std::move(moved.status);
        key = std::move(moved.key);
        value = std::move(moved.value);
        isBlob = std::move(moved.isBlob);
      }
      return *this;
    }

    std::string key;
    std::string value;
    Status status;
    std::future<bool> isBlob;
    bool forward;
    bool valid;
  };

  void GetFromPrefetched(bool forward) EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    mutex_.AssertHeld();
    while (true) {
      if (forward_ == forward) {
        while (!end_ && fetched_.empty()) {
          cv_.Wait();
        }

        if (fetched_.empty()) {
          assert(!Valid());
          return;
        }

        if (fetched_.front().isBlob.valid()) fetched_.front().isBlob.get();
        current_ = std::move(fetched_.front());
        fetched_.pop_front();

        // batch reap
        // TODO(optimize): size based batch reap
        for (auto&& item : fetched_) {
          if (item.isBlob.valid()) {
            item.isBlob.get();
          }
          finished_.emplace_back(std::move(item));
        }
        fetched_.clear();

        cv_.Signal();
        return;
      } else {  // change direction
        ChangeDirection();
        cv_.Signal();
      }
    }
  }

  void WaitAndClear() EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    mutex_.AssertHeld();
    for (auto& item : fetched_) {
      if (item.isBlob.valid()) {
        item.isBlob.wait();
      }
    }
    finished_.clear();
    fetched_.clear();
  }

  void ChangeDirection() EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    mutex_.AssertHeld();
    iter_->Seek(current_.key);
    forward_ = !forward_;
    end_ = false;
    if (forward_) {
      iter_->Next();
    } else {
      iter_->Prev();
    }
    WaitAndClear();
  }

  void MaybeIndirectValue() {
    mutex_.AssertHeld();
    if (iter_->Valid() && iter_->valueType() == kTypeValueHandle) {
      ValueHandle handle;
      Slice input(iter_->value());
      handle.DecodeFrom(&input);
      vlog_->Get(ReadOptions(), handle, &indirect_value_);
      isValueHandle_ = true;
    } else {
      isValueHandle_ = false;
    }
  }

  static void BGWork(void* arg) {
    reinterpret_cast<BlobDBIter*>(arg)->BGPrefetch();
  }

  void BGPrefetch() {
    while (true) {
      MutexLock l(&mutex_);

      while (!closed_ && iter_->Valid() && fetched_.size() >= max_prefetch_) {
        cv_.Wait();
      }

      if (closed_) {
        bg_thread_finished_ = true;
        cv_.Signal();
        break;
      }

      while (fetched_.size() < max_prefetch_) {
        fetched_.emplace_back();
        Item& item = fetched_.back();
        item.forward = forward_;

        if (!iter_->Valid()) {
          end_ = true;
          item.valid = false;
          item.status = iter_->status();
          break;
        }

        // TODO(optimize): use placement new to avoid frequent small allocation
        auto* p = new std::promise<bool>;

        item.valid = true;
        item.key.assign(iter_->key().data(), iter_->key().size());
        item.value.assign(iter_->value().data(), iter_->value().size());
        item.isBlob = p->get_future();
        if (iter_->valueType() != kTypeValueHandle) {
          p->set_value(false);
          delete p;
        } else {
          auto task =
              +[](Item* arg, ValueLogImpl* vlog, std::promise<bool>* p) {
                Item* item = reinterpret_cast<Item*>(arg);
                ValueHandle handle;
                Slice input(item->value);
                handle.DecodeFrom(&input);
                item->status = vlog->Get(ReadOptions(), handle, &item->value);
                if (!item->status.ok()) {
                  item->valid = false;
                }
                p->set_value(true);
                delete p;
              };
          env_->SubmitJob(std::bind(task, &item, vlog_, p));
        }

        if (forward_) {
          iter_->Next();
        } else {
          iter_->Prev();
        }
      }

      cv_.Signal();
    }
  }

  Env* const env_;
  DBIter* iter_ GUARDED_BY(mutex_);
  ValueLogImpl* vlog_;
  mutable Item current_;
  std::string indirect_value_;
  bool isValueHandle_;

  bool prefetch_;
  bool closed_ GUARDED_BY(mutex_);
  bool end_
      GUARDED_BY(mutex_);  // whether the prefetch thread hit an !iter_->Valid()
  bool forward_;           // iterate direction
  bool bg_thread_finished_ GUARDED_BY(mutex_);
  int max_prefetch_;

  mutable port::Mutex mutex_;
  port::CondVar cv_ GUARDED_BY(mutex_);
  std::deque<Item> fetched_ GUARDED_BY(mutex_);
  std::deque<Item> finished_;
  void* buf_;
};

}  // anonymous namespace

Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator,
                        Iterator* internal_iter, SequenceNumber sequence,
                        uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

Iterator* NewBlobDBIterator(DBImpl* db, ValueLogImpl* vlog, Env* env,
                            const Comparator* user_key_comparator,
                            Iterator* internal_iter, SequenceNumber sequence,
                            uint32_t seed, bool prefetch, int max_prefetch) {
  DBIter* dbiter =
      new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
  return new BlobDBIter(dbiter, vlog, env, prefetch, max_prefetch);
}

}  // namespace leveldb
