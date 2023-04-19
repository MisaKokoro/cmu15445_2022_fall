//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  //   throw NotImplementedException(
  //       "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //       "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lk(latch_);
  if (!free_list_.empty()) {
    page_id_t new_page_id = AllocatePage();

    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();

    AddFrame(frame_id, new_page_id);
    Page *new_page = &pages_[frame_id];
    *page_id = new_page_id;
    return new_page;
  }

  frame_id_t frame_id = -1;
  if (replacer_->Evict(&frame_id)) {
    EvictFrame(frame_id);

    page_id_t new_page_id = AllocatePage();
    AddFrame(frame_id, new_page_id);
    Page *new_page = &pages_[frame_id];
    *page_id = new_page_id;
    return new_page;
  }

  return nullptr;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lk(latch_);
  frame_id_t frame_id = -1;
  // 如果当前页面在buffer中直接返回
  if (page_table_->Find(page_id, frame_id)) {
    BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(pool_size_), "frame_id should be valid");
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }
  // 需要从磁盘上将page读入到bufferpool
  // 1. 可以找到一个空闲的frame
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

    AddFrame(frame_id, page_id);
    return &pages_[frame_id];
  }

  // 2. 可以找到一个被驱逐的frame
  if (replacer_->Evict(&frame_id)) {
    EvictFrame(frame_id);
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    AddFrame(frame_id, page_id);
    return &pages_[frame_id];
  }

  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount() == 0) {
    return false;
  }

  pages_[frame_id].pin_count_--;
  pages_[frame_id].is_dirty_ |= is_dirty;  // 为什么不是直接设置为is_dirty
  // 注意这里要求的是should be evictable而不是真的被驱逐
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  return WritePageToDisk(page_id);
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lk(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID) {
      WritePageToDisk(pages_[i].GetPageId());
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
  ResetFrame(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::ResetFrame(frame_id_t frame_id) -> void {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(pool_size_), "frame_id should be valid");
  Page &page = pages_[frame_id];
  page.ResetMemory();
  page.is_dirty_ = false;
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
}

auto BufferPoolManagerInstance::AddFrame(frame_id_t frame_id, page_id_t page_id) -> void {
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;
  pages_[frame_id].page_id_ = page_id;
  page_table_->Insert(page_id, frame_id);
}

auto BufferPoolManagerInstance::EvictFrame(frame_id_t frame_id) -> void {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(pool_size_), "frame_id should be valid");
  page_id_t page_id = pages_[frame_id].GetPageId();
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  ResetFrame(frame_id);
  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
}

auto BufferPoolManagerInstance::WritePageToDisk(page_id_t page_id) -> bool {
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page &page = pages_[frame_id];
  disk_manager_->WritePage(page_id, page.GetData());
  page.is_dirty_ = false;
  return true;
}
}  // namespace bustub
