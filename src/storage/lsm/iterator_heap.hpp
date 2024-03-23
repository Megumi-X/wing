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
    bool ret = heap_.top()->Valid();
    return ret;
 }

  Slice key() override {
    Slice ret = heap_.top()->key(); 
    return ret;
  }

  Slice value() override {
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
