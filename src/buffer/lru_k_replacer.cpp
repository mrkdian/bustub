//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdint>
#include <ctime>

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "fmt/core.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  this->latch_.lock();

  if(!this->young_list_.empty()) {
    auto it = this->young_list_.begin();
    *frame_id = it->fid_;
    this->node_store_.erase(it->fid_);
    this->young_list_.erase(it);

    this->latch_.unlock();
    return true;
  }

  if(!this->old_list_.empty()) {
    auto it = this->old_list_.begin();
    *frame_id = it->fid_;
    this->node_store_.erase(it->fid_);
    this->old_list_.erase(it);

    this->latch_.unlock();
    return true;
  }

  this->latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (static_cast<uint64_t>(frame_id) > this->replacer_size_) {
    throw Exception(fmt::format("LRUKReplacer record access, get frame_id {} larger than num_frames {}", frame_id, this->replacer_size_));
  }

  this->latch_.lock();

  auto it = this->node_store_.find(frame_id);

  size_t cur_ts = time(nullptr);

  std::list<LRUKNode>::iterator node_it;
  if (it == this->node_store_.end()) {
    this->young_list_.emplace_back(cur_ts, 1, frame_id);
    node_it = --this->young_list_.end();
    this->node_store_[frame_id] = node_it;
  } else {
    node_it = it->second;
    if(node_it->k_ < this->k_) {
      node_it->k_++;
    }
    node_it->last_ts_ = cur_ts;
  }

  if(!node_it->is_evictable_) {
    if(node_it->k_ >= this->k_) {
      node_it->is_old_ = true;
    }
    this->latch_.unlock();
    return;
  }

  if(!node_it->is_old_ && node_it->k_ >= this->k_) {
    this->old_list_.emplace_back(node_it->last_ts_, node_it->k_, node_it->fid_, node_it->is_evictable_, true);
    this->young_list_.erase(node_it);
    node_it = this->old_list_.end();
    node_it--;
    this->node_store_[frame_id] = node_it;
  }
  this->latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<uint64_t>(frame_id) > this->replacer_size_) {
    throw Exception(fmt::format("LRUKReplacer set evictable, get frame_id {} larger than num_frames {}", frame_id, this->replacer_size_));
  }

  this->latch_.lock();

  auto it = this->node_store_.find(frame_id);

  if(it == this->node_store_.end()) {
    this->latch_.unlock();
    throw Exception(fmt::format("LRUKReplacer set evictable, invalid frame id {}", frame_id));
  }

  auto node_it = it->second;

  if(node_it->is_evictable_ && !set_evictable) {
    this->pin_list_.emplace_back(node_it->last_ts_, node_it->k_, node_it->fid_, false, node_it->is_old_);
    if(node_it->is_old_) {
      this->old_list_.erase(node_it);
    } else {
      this->young_list_.erase(node_it);
    }
    this->node_store_[frame_id] = --this->pin_list_.end();
    this->latch_.unlock();
    return;
  }

  if(!node_it->is_evictable_ && set_evictable) {
    if(node_it->is_old_) {
      this->old_list_.emplace_back(node_it->last_ts_, node_it->k_, node_it->fid_, true, node_it->is_old_);
      this->node_store_[frame_id] = --this->old_list_.end();
    } else {
      this->young_list_.emplace_back(node_it->last_ts_, node_it->k_, node_it->fid_, true, node_it->is_old_);
      this->node_store_[frame_id] = --this->young_list_.end();
    }
    this->pin_list_.erase(node_it);
  }
  this->latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  this->latch_.lock();

  auto it = this->node_store_.find(frame_id);
  if(it == this->node_store_.end()) {
    this->latch_.unlock();
    return;
  }

  auto &node_it = it->second;
  if(!node_it->is_evictable_) {
    this->latch_.unlock();
    throw Exception(fmt::format("LRUKReplacer remove, unevictable frame {}", frame_id));
  }

  if(node_it->is_old_) {
    this->old_list_.erase(node_it);
  } else {
    this->young_list_.erase(node_it);
  }
  this->node_store_.erase(it);
  this->latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  this->latch_.lock();
  auto size = this->young_list_.size() + this->old_list_.size();
  this->latch_.unlock();
  return size;
}

}  // namespace bustub
