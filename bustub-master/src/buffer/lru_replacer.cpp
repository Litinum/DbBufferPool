//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  if (frames.empty()) return false;
  *frame_id = frames.front();
  frames.pop_front();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  for (auto i = frames.begin(); i != frames.end(); i++) {
    if ((frame_id_t)*i == frame_id) {
      frames.erase(i);
      return;
    }
  }
  // TODO :: Not found
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  auto i = std::find(frames.begin(), frames.end(), frame_id);
  if (i != frames.end()) frames.erase(i);
  frames.push_back(frame_id);
}

auto LRUReplacer::Size() -> size_t { return frames.size(); }

}  // namespace bustub
