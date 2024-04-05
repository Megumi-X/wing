#include "storage/lsm/level.hpp"
#include <iostream>

namespace wing {

namespace lsm {

GetResult SortedRun::Get(Slice key, uint64_t seq, std::string* value, seq_t* latest_seq) {
  const auto sst_index = std::lower_bound(ssts_.begin(), ssts_.end(), key, [&](const std::shared_ptr<SSTable>& sst1, const Slice& key) {
    return sst1->GetLargestKey() < ParsedKey(key, seq, RecordType::Value);
  });
  if (sst_index == ssts_.end()) {
    return GetResult::kNotFound;
  }
  return sst_index->get()->Get(key, seq, value, latest_seq);
}

SortedRunIterator SortedRun::Seek(Slice key, uint64_t seq) {
  const auto sst_index = std::lower_bound(ssts_.begin(), ssts_.end(), key, [&](const std::shared_ptr<SSTable>& sst1, const Slice& key) {
    return sst1->GetLargestKey() < ParsedKey(key, seq, RecordType::Value);
  });
  if (sst_index == ssts_.end()) {
    return Begin();
  }
  auto iter = sst_index->get()->Seek(key, seq);
  return SortedRunIterator(this, std::move(iter), sst_index - ssts_.begin());
}

SortedRunIterator SortedRun::Begin() {
  SortedRunIterator iter(this, ssts_[0]->Begin(), 0);
  iter.SeekToFirst();
  return iter;
}

SortedRun::~SortedRun() {
  if (remove_tag_) {
    for (auto sst : ssts_) {
      sst->SetRemoveTag(true);
    }
  }
}

void SortedRunIterator::SeekToFirst() {
  sst_it_ = run_->GetSSTs()[0]->Begin();
  sst_id_ = 0;
}

void SortedRunIterator::Seek(Slice key, uint64_t seq) {
  const auto sst_index = std::lower_bound(run_->GetSSTs().begin(), run_->GetSSTs().end(), key, [&](const std::shared_ptr<SSTable>& sst1, const Slice& key) {
    return sst1->GetLargestKey() < ParsedKey(key, seq, RecordType::Value);
  });
  if (sst_index == run_->GetSSTs().end()) {
    SeekToFirst();
    return;
  }
  sst_id_ = sst_index - run_->GetSSTs().begin();
  sst_it_ = run_->GetSSTs()[sst_id_]->Seek(key, seq);
}

bool SortedRunIterator::Valid() { return sst_it_.Valid(); }

Slice SortedRunIterator::key() const { return sst_it_.key(); }

Slice SortedRunIterator::value() const { return sst_it_.value(); }

void SortedRunIterator::Next() {
  sst_it_.Next();
  if (sst_it_.Valid()) return;
  if (sst_id_ >= run_->GetSSTs().size() - 1) return;
  sst_id_++;
  sst_it_ = run_->GetSSTs()[sst_id_]->Begin();
}

GetResult Level::Get(Slice key, uint64_t seq, std::string* value) {
  seq_t latest_seq = 0;
  GetResult ret = GetResult::kNotFound;
  for (int i = runs_.size() - 1; i >= 0; --i) {
    auto res = runs_[i]->Get(key, seq, value, &latest_seq);
    if (res != GetResult::kNotFound) {
      if (seq > latest_seq) {
        latest_seq = seq;
        ret = res;
      }
    }
  }
  return ret;
}

void Level::Append(std::vector<std::shared_ptr<SortedRun>> runs) {
  for (auto& run : runs) {
    size_ += run->size();
  }
  runs_.insert(runs_.end(), std::make_move_iterator(runs.begin()),
      std::make_move_iterator(runs.end()));
}

void Level::Append(std::shared_ptr<SortedRun> run) {
  size_ += run->size();
  runs_.push_back(std::move(run));
}

}  // namespace lsm

}  // namespace wing
