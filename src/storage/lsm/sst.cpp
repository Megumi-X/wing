#include "storage/lsm/sst.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>

#include "common/bloomfilter.hpp"

#include <iostream>

namespace wing {

namespace lsm {

SSTable::SSTable(SSTInfo sst_info, size_t block_size, bool use_direct_io)
  : sst_info_(std::move(sst_info)), block_size_(block_size) {
  file_ = std::make_unique<ReadFile>(sst_info_.filename_, use_direct_io);
  std::vector<size_t> index_offset;
  FileReader fr = FileReader(file_.get(), sst_info_.size_, sst_info_.index_offset_);
  size_t block_count = fr.ReadValue<size_t>();
  for (size_t i = 0; i < block_count + 1; i++) {
    index_offset.push_back(fr.ReadValue<size_t>());
  }
  for (size_t i = 0; i < block_count; i++) {
    IndexValue index_value;
    size_t len = index_offset[i + 1] - index_offset[i] - sizeof(BlockHandle);
    index_value.key_ = InternalKey(fr.ReadString(len));
    index_value.block_ = fr.ReadValue<BlockHandle>();
    index_.push_back(index_value); 
  }
  std::sort(index_.begin(), index_.end(), [](const IndexValue& a, const IndexValue& b) {
    return ParsedKey(a.key_) < ParsedKey(b.key_);
  });
  size_t bloom_len = fr.ReadValue<size_t>();
  
  bloom_filter_ = fr.ReadString(bloom_len);
  size_t max_len = fr.ReadValue<size_t>();
  largest_key_ = InternalKey(fr.ReadString(max_len));
  size_t min_len = fr.ReadValue<size_t>();
  smallest_key_ = InternalKey(fr.ReadString(min_len));
  
}

SSTable::~SSTable() {
  if (remove_tag_) {
    file_.reset();
    std::filesystem::remove(sst_info_.filename_);
  }
}

GetResult SSTable::Get(Slice key, uint64_t seq, std::string* value) {
  utils::BloomFilter filter;
  if (!filter.Find(key, bloom_filter_)) {
    // std::cout << "Negative with key: " << key << "\n";
    return GetResult::kNotFound;
  }
  const auto block_index = std::lower_bound(index_.begin(), index_.end(), key, [&](const IndexValue& index_value, const Slice& key) {
    return ParsedKey(index_value.key_) < ParsedKey(key, seq, RecordType::Value);
  });
  if (block_index == index_.end()) return GetResult::kNotFound;
  FileReader fr(file_.get(), 2 * block_size_, block_index->block_.offset_);
  AlignedBuffer buf(block_size_, 4096);
  fr.Read(buf.data(), block_index->block_.size_);
  BlockIterator block_it(buf.data(), block_index->block_);
  for (size_t i = 0; i < block_index->block_.count_; i++) {
    if (ParsedKey(block_it.key()).user_key_ == key && ParsedKey(block_it.key()).seq_ <= seq) {
      if (block_it.Valid()) {
        *value = block_it.value();
        return GetResult::kFound;
      } else {
        return GetResult::kDelete;
      }
    }
    block_it.Next();
  }
  // std::cout << "False Positive with key: " << key << "\n";
  return GetResult::kNotFound;
}

SSTableIterator SSTable::Seek(Slice key, uint64_t seq) {
  SSTableIterator iter(this);
  iter.Seek(key, seq);
  return iter;
}

SSTableIterator SSTable::Begin() {
  SSTableIterator iter(this);
  iter.SeekToFirst();
  return iter;
}

void SSTableIterator::Seek(Slice key, uint64_t seq) {
  SeekToFirst();
  const auto block_index = std::lower_bound(sst_->index_.begin(), sst_->index_.end(), key, [&](const IndexValue& index_value, const Slice& key) {
    return ParsedKey(index_value.key_) < ParsedKey(key, seq, RecordType::Value);
  });
  if (block_index == sst_->index_.end()) {
    // block_id_ = sst_->index_.size() - 1;
    // FileReader fr(sst_->file_.get(), 2 * sst_->block_size_, sst_->index_[block_id_].block_.offset_);
    // buf_ = AlignedBuffer(sst_->block_size_, 4096);
    // fr.Read(buf_.data(), sst_->index_[block_id_].block_.size_);
    // block_it_ = BlockIterator(buf_.data(), sst_->index_[block_id_].block_);
    return;
  }
  block_id_ = block_index - sst_->index_.begin();
  FileReader fr(sst_->file_.get(), 2 * sst_->block_size_, sst_->index_[block_id_].block_.offset_);
  buf_ = AlignedBuffer(sst_->block_size_, 4096);
  fr.Read(buf_.data(), sst_->index_[block_id_].block_.size_);
  block_it_ = BlockIterator(buf_.data(), sst_->index_[block_id_].block_);
  block_it_.Seek(key, seq);
}

void SSTableIterator::SeekToFirst() {
  FileReader fr = FileReader(sst_->file_.get(), 2 * sst_->block_size_, 0);
  fr.Read(buf_.data(), sst_->index_[0].block_.size_);
  block_it_ = BlockIterator(buf_.data(), sst_->index_[0].block_);
  block_id_ = 0;
  block_it_.SeekToFirst();
}

bool SSTableIterator::Valid() { return block_it_.Valid(); }

Slice SSTableIterator::key() const { return block_it_.key(); }

Slice SSTableIterator::value() const { return block_it_.value(); }

void SSTableIterator::Next() {
  block_it_.Next();
  if (block_it_.Valid()) return;
  if (block_id_ >= sst_->index_.size() - 1) return;
  block_id_++;
  BlockHandle handle = sst_->index_[block_id_].block_;
  FileReader fr(sst_->file_.get(), 2 * sst_->block_size_, handle.offset_);
  buf_ = AlignedBuffer(sst_->block_size_, 4096);
  fr.Read(buf_.data(), handle.size_);
  block_it_ = BlockIterator(buf_.data(), handle);
  block_it_.SeekToFirst();
}

void SSTableBuilder::Append(ParsedKey key, Slice value) {
  utils::BloomFilter filter;
  key_hashes_.push_back(filter.BloomHash(key.user_key_));
  if (!block_builder_.Append(key, value)) {
    block_builder_.Finish();
    transfor_data_from_block_builder();
    block_builder_.Clear();
    block_builder_.Append(key, value);
  }
}

void SSTableBuilder::Finish() {
  block_builder_.Finish();
  transfor_data_from_block_builder();
  size_t offset = index_offset_ + (index_data_.size() + 2) * sizeof(size_t);
  writer_->AppendValue<size_t>(index_data_.size());
  for (const auto & index_value : index_data_) {
    writer_->AppendValue<size_t>(offset);
    offset += index_value.key_.size() + sizeof(BlockHandle);
  }
  writer_->AppendValue<size_t>(offset);
  for (const auto& index_value : index_data_) {
    writer_->AppendString(index_value.key_.GetSlice());
    writer_->AppendValue<BlockHandle>(index_value.block_);
  }
  std::string bloom_filter;
  utils::BloomFilter filter;
  filter.Create(count_, bloom_bits_per_key_, bloom_filter);
  for (const auto& hash : key_hashes_){
    filter.Add(hash, bloom_filter);
  }
  writer_->AppendValue<size_t>(bloom_filter.size());
  writer_->AppendString(bloom_filter);
  writer_->AppendValue<size_t>(largest_key_.size());
  writer_->AppendString(largest_key_.GetSlice());
  writer_->AppendValue<size_t>(smallest_key_.size());
  writer_->AppendString(smallest_key_.GetSlice());
  writer_->AppendValue<size_t>(index_offset_);
  writer_->AppendValue<size_t>(bloom_filter_offset_ + 2 * sizeof(size_t));
  writer_->AppendValue<size_t>(count_);
  writer_->Flush();
}

void SSTableBuilder::transfor_data_from_block_builder() {
  IndexValue index_value;
  index_value.key_ = block_builder_.largest_key;
  index_value.block_.count_ = block_builder_.count();
  index_value.block_.size_ = block_builder_.size();
  index_value.block_.offset_ = current_block_offset_;
  index_data_.push_back(index_value);

  if (block_builder_.largest_key > ParsedKey(largest_key_) || index_data_.size() == 1)
    largest_key_ = InternalKey(block_builder_.largest_key);
  if (block_builder_.smallest_key < ParsedKey(smallest_key_) || index_data_.size() == 1)
    smallest_key_ = InternalKey(block_builder_.smallest_key);
  
  index_offset_ += block_builder_.size();
  count_ += block_builder_.count();
  current_block_offset_ += block_builder_.size();
  bloom_filter_offset_ += block_builder_.size() + index_value.key_.size() + sizeof(BlockHandle) + sizeof(size_t);
}

}  // namespace lsm

}  // namespace wing
