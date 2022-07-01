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

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  for (Page *page = pages_; page < pages_ + pool_size_; page++) {
    if (page->GetPageId() == page_id) {
      if (page->IsDirty() == false) return true;

      page->WLatch();
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->WUnlatch();

      return true;
    }
  }

  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (Page *page = pages_; page < pages_ + pool_size_; page++) {
    if (page->IsDirty() == true) {
      page->WLatch();
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->WUnlatch();
    }
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  frame_id_t id;

  latch_.lock();
  if (free_list_.size() > 0) {
    id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (replacer_->Victim(&id) == false) {
      latch_.unlock();
      return nullptr;
    }
    if (pages_[id].IsDirty()) {
      pages_[id].WLatch();
      disk_manager_->WritePage(pages_[id].GetPageId(), pages_[id].GetData());
      pages_[id].WUnlatch();
    }
  }
  latch_.unlock();

  latch_.lock();
  *page_id = AllocatePage();
  latch_.unlock();

  pages_[id].WLatch();
  pages_[id].SetPageId(*page_id);
  pages_[id].SetPinCount(1);
  pages_[id].SetDirty(true);
  pages_[id].ResetData();
  pages_[id].WUnlatch();

  return &pages_[id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  latch_.lock();

  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == page_id) {
      latch_.lock();
      replacer_->Pin(i);
      pages_[i].SetPinCount(pages_[i].GetPinCount() + 1);

      latch_.unlock();

      return &pages_[i];
    }
  }
  latch_.unlock();

  frame_id_t id;

  latch_.lock();
  if (free_list_.size() > 0) {
    id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (replacer_->Victim(&id) == false) {
      latch_.unlock();
      return nullptr;
    }
  }
  latch_.unlock();

  if (pages_[id].IsDirty() == true) {
    pages_[id].WLatch();
    disk_manager_->WritePage(pages_[id].GetPageId(), pages_[id].GetData());
    pages_[id].WUnlatch();
  }

  // DeletePgImp(pages_[id].GetPageId());
  // DeallocatePage(pages_[id].GetPageId());
  pages_[id].SetPageId(page_id);
  pages_[id].SetPinCount(1);
  pages_[id].SetDirty(false);
  char *data = new char[PAGE_SIZE];
  pages_[id].RLatch();
  disk_manager_->ReadPage(page_id, data);
  pages_[id].RUnlatch();
  pages_[id].SetData(data);

  return &pages_[id];
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  latch_.lock();
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == page_id) {
      if (pages_[i].GetPinCount() != 0) {
        latch_.unlock();

        return false;
      }

      else {
        DeallocatePage(pages_[i].GetPageId());
        pages_[i].SetPageId(INVALID_PAGE_ID);
        pages_[i].SetPinCount(0);
        pages_[i].SetDirty(false);
        free_list_.push_front(i);
      }
    }
  }
  latch_.unlock();

  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == page_id) {
      if (pages_[i].GetPinCount() == 0) {
        latch_.unlock();

        return false;
      }

      pages_[i].SetPinCount(pages_[i].GetPinCount() - 1);

      if (pages_[i].GetPinCount() == 0) {
        replacer_->Unpin(i);
      }

      if (is_dirty == true) pages_[i].SetDirty(true);

      latch_.unlock();

      return true;
    }
  }
  latch_.unlock();

  return false;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);

  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
