#include "storage/lsm/block.hpp"
#include <iostream>
namespace wing {

namespace lsm {

bool BlockBuilder::Append(ParsedKey key, Slice value) {
  offset_t key_length = key.size();
  offset_t value_length = value.size();
  size_t append_size = key_length + sizeof(offset_t) + value_length + sizeof(offset_t) + sizeof(offset_t);
  if (current_size_ + append_size > block_size_) {
    return false;
  }
  offsets_.push_back(offset_);
  offset_ += append_size - sizeof(offset_t);
  file_->AppendValue<offset_t>(key_length);
  auto ikey = InternalKey(key);
  file_->AppendString(ikey.GetSlice());
  file_->AppendValue<offset_t>(value_length);
  file_->AppendString(value);
  current_size_ += append_size;
  if (offsets_.size() <= 1 || ParsedKey(largest_key) < key){
    largest_key = std::move(ikey);
  } 
  // if (offsets_.size() <= 1 || ParsedKey(smallest_key) > key){
  //   smallest_key = ikey;
  // }
  // file_->Flush();
  return true;
}

void BlockBuilder::Finish() {
  for (auto offset : offsets_) {
    file_->AppendValue<offset_t>(offset);
  }
  // file_->Flush();
}

void BlockIterator::Seek(Slice user_key, seq_t seq) {
  char* offset_iter = const_cast<char*>(data_) + handle_.size_ - handle_.count_ * sizeof(offset_t);
  for (size_t i = 0; i < handle_.count_; i++) {
    offset_t offset = *reinterpret_cast<const offset_t*>(offset_iter + i * sizeof(offset_t));
    offset_t key_length = *reinterpret_cast<const offset_t*>(data_ + offset);
    Slice key = Slice(data_ + offset + sizeof(offset_t), key_length);
    if (ParsedKey(key) >= ParsedKey(user_key, seq, RecordType::Value)) {
      current_ = const_cast<char*>(data_ + offset);
      count_ = i;
      return;
    }
  }
}

void BlockIterator::SeekToFirst() { current_ = const_cast<char*>(data_); count_ = 0;}

Slice BlockIterator::key() const {
  if (data_ == nullptr || current_ == nullptr) return "INVALID";
  offset_t key_length = *reinterpret_cast<const offset_t*>(current_);
  return Slice(current_ + sizeof(offset_t), key_length);
}

Slice BlockIterator::value() const {
  if (data_ == nullptr || current_ == nullptr) return "INVALID";
  offset_t key_length = *reinterpret_cast<const offset_t*>(current_);
  offset_t value_length = *reinterpret_cast<const offset_t*>(current_ + sizeof(offset_t) + key_length);
  return Slice(current_ + sizeof(offset_t) + key_length + sizeof(offset_t), value_length);
}

void BlockIterator::Next() {
  offset_t key_length = *reinterpret_cast<const offset_t*>(current_);
  offset_t value_length = *reinterpret_cast<const offset_t*>(current_ + sizeof(offset_t) + key_length);
  current_ += sizeof(offset_t) + key_length + sizeof(offset_t) + value_length;
  count_++;
}

bool BlockIterator::Valid() {
  // if (data_ == nullptr || current_ == nullptr) return false;
  // offset_t key_length = *reinterpret_cast<const offset_t*>(current_);
  // Slice key = Slice(current_ + sizeof(offset_t), key_length);
  // return (ParsedKey(key).type_ == RecordType::Value);
  return count_ < handle_.count_;
}

}  // namespace lsm

}  // namespace wing
