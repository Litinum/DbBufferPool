//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  resident = boost::dynamic_bitset(num_pages);
  clock = boost::dynamic_bitset(num_pages);
}

ClockReplacer::~ClockReplacer() = default;

auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool {
  if (resident.any() == false) return false;
  while (true) {
    if (resident[current_frame]) {
      if (clock[current_frame])
        clock[current_frame] = 0;
      else {
        *frame_id = current_frame;
        current_frame = (current_frame + 1) % clock.size();
        return true;
      }
    }
    current_frame = (current_frame + 1) % clock.size();
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) { resident[frame_id] = 0; }

void ClockReplacer::Unpin(frame_id_t frame_id) {
  resident[frame_id] = 1;
  clock[frame_id] = 1;
}

auto ClockReplacer::Size() -> size_t { return resident.count(); }

}  // namespace bustub
