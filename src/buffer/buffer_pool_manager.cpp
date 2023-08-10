//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <mutex>

#include "buffer/buffer_pool_manager.h"

#include "common/logger.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock lk(this->latch_);

  if(!this->free_list_.empty()) {
    auto frame_id = this->free_list_.front();
    this->free_list_.pop_front();

    auto new_page_id = AllocatePage();

    this->pages_[frame_id].page_id_ = new_page_id;
    this->pages_[frame_id].pin_count_ = 1;
    this->pages_[frame_id].is_dirty_ = false;
    this->pages_[frame_id].ResetMemory();

    this->page_table_[new_page_id] = frame_id;
    
    this->replacer_->RecordAccess(frame_id);
    this->replacer_->SetEvictable(frame_id, false);

    return this->pages_ + frame_id;
  }

  if(this->replacer_->Size() > 0) {
    frame_id_t frame_id;
    auto is_evict = this->replacer_->Evict(&frame_id);
    if(!is_evict) {
      LOG_DEBUG("BufferPoolManager NewPage evict false");
      return nullptr;
    }
    if(this->pages_[frame_id].pin_count_ > 0) {
      LOG_DEBUG("BufferPoolManager NewPage evict page, but the pin count > 0");
    }
    
    if(this->pages_[frame_id].is_dirty_) {
      this->disk_manager_->WritePage(this->pages_[frame_id].page_id_, this->pages_[frame_id].data_);
    }
    this->page_table_.erase(this->pages_[frame_id].page_id_);
    
    auto new_page_id = AllocatePage();

    this->pages_[frame_id].page_id_ = new_page_id;
    this->pages_[frame_id].pin_count_ = 1;
    this->pages_[frame_id].is_dirty_ = false;
    this->pages_[frame_id].ResetMemory();

    this->page_table_[new_page_id] = frame_id;

    this->replacer_->RecordAccess(frame_id);
    this->replacer_->SetEvictable(frame_id, false);

    return this->pages_ + frame_id;
  }

  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock lk(this->latch_);

  auto it = this->page_table_.find(page_id);
  if(it != this->page_table_.end()) {
    return this->pages_ + it->second;
  }

  if(!this->free_list_.empty()) {
    auto frame_id = this->free_list_.front();
    this->free_list_.pop_front();

    this->pages_[frame_id].page_id_ = page_id;
    this->pages_[frame_id].pin_count_ = 1;
    this->pages_[frame_id].is_dirty_ = false;
    this->disk_manager_->ReadPage(page_id, this->pages_[frame_id].data_);

    this->page_table_[page_id] = frame_id;

    this->replacer_->SetEvictable(frame_id, false);
    this->replacer_->RecordAccess(frame_id);

    return this->pages_ + frame_id;
  }

  if(this->replacer_->Size() > 0) {
    frame_id_t frame_id;
    auto is_evict = this->replacer_->Evict(&frame_id);
    if(!is_evict) {
      LOG_DEBUG("BufferPoolManager FetchPage evict false");
      return nullptr;
    }
    if(this->pages_[frame_id].pin_count_ > 0) {
      LOG_DEBUG("BufferPoolManager FetchPage evict page, but the pin count > 0");
    }
    
    if(this->pages_[frame_id].is_dirty_) {
      this->disk_manager_->WritePage(this->pages_[frame_id].page_id_, this->pages_[frame_id].data_);
    }
    this->page_table_.erase(this->pages_[frame_id].page_id_);
    
    this->disk_manager_->ReadPage(page_id, this->pages_[frame_id].data_);
    this->page_table_[page_id] = frame_id;

    this->pages_[frame_id].pin_count_++;
    this->replacer_->SetEvictable(frame_id, false);
    this->replacer_->RecordAccess(frame_id);

    return this->pages_ + frame_id;
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { return false; }

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
