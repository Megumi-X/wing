#pragma once

#include <queue>
#include <functional>

#include "storage/lsm/format.hpp"
#include "storage/lsm/iterator.hpp"

namespace wing {

namespace lsm {

template <typename T>
class IteratorHeap final : public Iterator {
 public:
  IteratorHeap() = default;

  void Push(T* it) {
    heap_.push(it);
  }

  void Build() {
    ;
  }

  bool Valid() override { 
    if (heap_.size() == 0) return false;
    bool ret = heap_.top()->Valid();
    return ret;
 }

  Slice key() const override {
    if (heap_.size() == 0) return "INVALID";
    Slice ret = heap_.top()->key(); 
    return ret;
  }

  Slice value() const override {
    if (heap_.size() == 0) return "INVALID";
    Slice ret = heap_.top()->value(); 
    return ret;
  }

  void Next() override {
    auto top = heap_.top();
    heap_.pop();
    top->Next();
    if (top->Valid()) {
        heap_.push(top);
    }
  }

  void Clear() {
    while (!heap_.empty()) {
        heap_.pop();
    }
  }

 private:
    struct Compare {
        bool operator()(T* lhs, T* rhs) const {
            return ParsedKey(lhs->key()) > ParsedKey(rhs->key());
        }
    };
    std::priority_queue<T*, std::vector<T*>, Compare> heap_;
};

}  // namespace lsm

}  // namespace wing
