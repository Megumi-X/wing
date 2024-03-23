#include "storage/lsm/level.hpp"

namespace wing {

namespace lsm {

GetResult SortedRun::Get(Slice key, uint64_t seq, std::string* value) {
  for (const auto& sst : ssts_) {
    auto res = sst->Get(key, seq, value);
    if (res != GetResult::kNotFound) {
      return res;
    }
  }
  return GetResult::kNotFound;
}

SortedRunIterator SortedRun::Seek(Slice key, uint64_t seq) {
  SortedRunIterator iter = Begin();
  while (iter.Valid()) {
    if (ParsedKey(iter.key()) >= ParsedKey(key, seq, RecordType::Value)) {
      return iter;
    }
    iter.Next();
  }
  return iter;
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
  record_id_ = 0;
}

bool SortedRunIterator::Valid() { return sst_it_.Valid(); }

Slice SortedRunIterator::key() { return sst_it_.key(); }

Slice SortedRunIterator::value() { return sst_it_.value(); }

void SortedRunIterator::Next() {
  sst_it_.Next();
  record_id_++;
  if (record_id_ < run_->GetSSTs()[sst_id_]->GetSSTInfo().count_) return;
  if (sst_id_ >= run_->GetSSTs().size() - 1) return;
  sst_id_++;
  record_id_ = 0;
  sst_it_ = run_->GetSSTs()[sst_id_]->Begin();
}

GetResult Level::Get(Slice key, uint64_t seq, std::string* value) {
  for (int i = runs_.size() - 1; i >= 0; --i) {
    auto res = runs_[i]->Get(key, seq, value);
    if (res != GetResult::kNotFound) {
      return res;
    }
  }
  return GetResult::kNotFound;
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
