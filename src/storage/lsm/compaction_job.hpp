#pragma once

#include "storage/lsm/sst.hpp"
#include <iostream>

namespace wing {

namespace lsm {

class CompactionJob {
 public:
  CompactionJob(FileNameGenerator* gen, size_t block_size, size_t sst_size,
      size_t write_buffer_size, size_t bloom_bits_per_key, bool use_direct_io)
    : file_gen_(gen),
      block_size_(block_size),
      sst_size_(sst_size),
      write_buffer_size_(write_buffer_size),
      bloom_bits_per_key_(bloom_bits_per_key),
      use_direct_io_(use_direct_io) {}

  /**
   * It receives an iterator and returns a list of SSTable
   */
  template <typename IterT>
  std::vector<SSTInfo> Run(IterT&& it) {
    std::vector<SSTInfo> sst_list;
    std::string last_user_key;
    seq_t last_seq = 0;
    auto file_info_pair = file_gen_->Generate();
    std::vector<std::unique_ptr<SSTableBuilder>> builders;
    builders.emplace_back(std::make_unique<SSTableBuilder>(std::make_unique<FileWriter>(std::make_unique<SeqWriteFile>(file_info_pair.first, use_direct_io_), write_buffer_size_), block_size_, bloom_bits_per_key_));
    // int count10 = 0;
    // int count11 = 0;
    // int count12 = 0;
    while (it.Valid()){
      // count10++;
      auto pkey = ParsedKey(it.key());
      auto current_user_key = pkey.user_key_;
      seq_t current_seq = pkey.seq_;
      if (current_user_key == last_user_key && current_seq < last_seq) {
        it.Next();
        continue;
      }
      size_t append_size = it.key().size() + it.value().size() + 3 * sizeof(offset_t);
      if (builders.back()->GetIndexOffset() + append_size > sst_size_) {
        builders.back()->Finish();
        SSTInfo sst_info;
        sst_info.sst_id_ = file_info_pair.second;
        sst_info.size_ = builders.back()->size();
        sst_info.index_offset_ = builders.back()->GetIndexOffset();
        sst_info.bloom_filter_offset_ = builders.back()->GetBloomFilterOffset();
        sst_info.count_ = builders.back()->count();
        sst_info.filename_ = file_info_pair.first;
        sst_list.emplace_back(sst_info);
        file_info_pair = file_gen_->Generate();
        builders.emplace_back(std::make_unique<SSTableBuilder>(std::make_unique<FileWriter>(std::make_unique<SeqWriteFile>(file_info_pair.first, use_direct_io_), write_buffer_size_), block_size_, bloom_bits_per_key_));
        // count11 += sst_info.count_;
        // std::cout << "count10: " << count10 << " count11: " << count11 << " count12: " << count12 << "\n";
      }
      if (pkey.type_ == RecordType::Value) {
        builders.back()->Append(ParsedKey(current_user_key, current_seq, RecordType::Value), it.value());  
        // count12 ++;    
      } else if (pkey.type_ == RecordType::Deletion) {
        builders.back()->Append(ParsedKey(current_user_key, current_seq, RecordType::Deletion), it.value());
        // count12++;
      } else {
        DB_ERR("WRONG");
      }
      last_user_key = current_user_key;
      last_seq = current_seq;
      it.Next();
    }
    builders.back()->Finish();
    if (builders.back()->count() == 0) {
      // std::cout << "count10: " << count10 << " count11: " << count11 << " count12: " << count12 << "\n";
      return sst_list;
    }
    SSTInfo sst_info;
    sst_info.sst_id_ = file_info_pair.second;
    sst_info.size_ = builders.back()->size();
    sst_info.index_offset_ = builders.back()->GetIndexOffset();
    sst_info.bloom_filter_offset_ = builders.back()->GetBloomFilterOffset();
    sst_info.count_ = builders.back()->count();
    // std::cout << "builder_count: " << builders.back()->count() << "\n";
    sst_info.filename_ = file_info_pair.first;
    sst_list.emplace_back(sst_info);
    // count11 += sst_info.count_;
    // std::cout << "count10: " << count10 << " count11: " << count11 << " count12: " << count12 << "\n";
    return sst_list;
  }

 private:
  /* Generate new SSTable file name */
  FileNameGenerator* file_gen_;
  /* The target block size */
  size_t block_size_;
  /* The target SSTable size */
  size_t sst_size_;
  /* The size of write buffer in FileWriter */
  size_t write_buffer_size_;
  /* The number of bits per key in bloom filter */
  size_t bloom_bits_per_key_;
  /* Use O_DIRECT or not */
  bool use_direct_io_;
};

}  // namespace lsm

}  // namespace wing
